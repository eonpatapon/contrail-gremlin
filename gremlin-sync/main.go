package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Jeffail/gabs"
	"github.com/eonpatapon/gremlin"
	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"
	"github.com/jawher/mow.cli"
	logging "github.com/op/go-logging"
	"github.com/streadway/amqp"
	"github.com/willfaught/gockle"
)

var (
	log    = logging.MustGetLogger("gremlin-sync")
	format = logging.MustStringFormatter(
		`%{color}%{time:15:04:05.000} %{shortfunc} ▶ %{level:.4s} %{id:03x}%{color:reset} %{message}`)
	gremlinClient *gremlin.Client
	noFlatten     = map[string]bool{
		"access_control_list_entries": true,
		"security_group_entries":      true,
		"vrf_assign_table":            true,
		"attr.ipam_subnets":           true,
	}
)

const (
	QueryMaxSize = 40000
	VncExchange  = "vnc_config.object-update"
	QueueName    = "gremlin.sync"
)

func parseJSON(valueJSON []byte) (*gabs.Container, error) {
	dec := json.NewDecoder(bytes.NewReader(valueJSON))
	dec.UseNumber()
	return gabs.ParseJSONDecoder(dec)
}

func propertiesQuery(props map[string]interface{}) (string, gremlin.Bind) {
	var buffer bytes.Buffer
	bindings := gremlin.Bind{}
	// Sort properties so that we generate identic queries
	propsName := make([]string, len(props))
	i := 0
	for propName := range props {
		propsName[i] = propName
		i++
	}
	sort.SliceStable(propsName, func(i, j int) bool {
		return propsName[i] < propsName[j]
	})
	for _, propName := range propsName {
		bindName := `_` + strings.Replace(propName, `.`, `_`, -1)
		buffer.WriteString(".property('")
		buffer.WriteString(propName)
		buffer.WriteString(`',`)
		buffer.WriteString(bindName)
		buffer.WriteString(`)`)
		bindings[bindName] = props[propName]
	}
	return buffer.String(), bindings
}

type Notification struct {
	Oper string `json:"oper"`
	Type string `json:"type"`
	UUID string `json:"uuid"`
}

type Edge struct {
	Source     string                 `json:"outV"`
	SourceType string                 `json:"outVLabel"`
	Target     string                 `json:"inV"`
	TargetType string                 `json:"inVLabel"`
	Type       string                 `json:"label"`
	Properties map[string]interface{} `json:"properties"`
}

func (e *Edge) AddProperties(prefix string, c *gabs.Container) {
	var pfix string

	if _, ok := noFlatten[prefix]; ok {
		e.AddProperty(prefix, c.String())
		return
	}

	if _, ok := c.Data().([]interface{}); ok {
		childs, _ := c.Children()
		for i, child := range childs {
			e.AddProperties(fmt.Sprintf("%s.%d", prefix, i), child)
		}
		return
	}
	if _, ok := c.Data().(map[string]interface{}); ok {
		childs, _ := c.ChildrenMap()
		for key, child := range childs {
			if prefix == "" {
				pfix = key
			} else {
				pfix = fmt.Sprintf("%s.%s", prefix, key)
			}
			e.AddProperties(pfix, child)
		}
		return
	}
	if str, ok := c.Data().(string); ok {
		e.AddProperty(prefix, str)
		return
	}
	if boul, ok := c.Data().(bool); ok {
		e.AddProperty(prefix, boul)
		return
	}
	if num, ok := c.Data().(json.Number); ok {
		if n, err := num.Int64(); err == nil {
			e.AddProperty(prefix, n)
			return
		}
		if n, err := num.Float64(); err == nil {
			e.AddProperty(prefix, n)
			return
		}
	}
	if prefix != "" {
		e.AddProperty(prefix, "null")
	}
}

func (e *Edge) AddProperty(prefix string, value interface{}) {
	if _, ok := e.Properties[prefix]; !ok {
		e.Properties[prefix] = value
	}
}

func (e Edge) Create() error {
	props, bindings := propertiesQuery(e.Properties)
	bindings["_src"] = e.Source
	bindings["_dst"] = e.Target
	bindings["_type"] = e.Type
	query := `g.V(_src).as('src').V(_dst).addE(_type).from('src')` + props + `.iterate()`
	_, err := gremlinClient.Send(
		gremlin.Query(query).Bindings(bindings),
	)
	if err == gremlin.ErrStatusInvalidRequestArguments {
		log.Errorf("Query: %s, Bindings: %s", query, bindings)
	}
	return err
}

func (e Edge) Exists() (exists bool, err error) {
	var (
		data []byte
		res  []bool
	)
	data, err = gremlinClient.Send(
		gremlin.Query(`g.V(src).out(type).hasId(dst).hasNext()`).Bindings(
			gremlin.Bind{
				"src":  e.Source,
				"dst":  e.Target,
				"type": e.Type,
			},
		),
	)
	if err != nil {
		return exists, err
	}
	json.Unmarshal(data, &res)
	return res[0], err
}

func (e Edge) Update() error {
	props, bindings := propertiesQuery(e.Properties)
	bindings["_src"] = e.Source
	bindings["_dst"] = e.Target
	query := `g.V(_src).bothE().where(otherV().hasId(_dst))`
	_, err := gremlinClient.Send(
		gremlin.Query(query + `.properties().drop()`).Bindings(bindings),
	)
	if err != nil {
		return err
	}
	_, err = gremlinClient.Send(
		gremlin.Query(query + props + `.iterate()`).Bindings(bindings),
	)
	if err == gremlin.ErrStatusInvalidRequestArguments {
		log.Errorf("Query: %s, Bindings: %s", query+props, bindings)
	}
	return err
}

func (e Edge) Delete() error {
	_, err := gremlinClient.Send(
		gremlin.Query("g.V(src).bothE().where(otherV().hasId(dst)).drop()").Bindings(
			gremlin.Bind{
				"src": e.Source,
				"dst": e.Target,
			},
		),
	)
	return err
}

type Vertex struct {
	ID         string                 `json:"id"`
	Type       string                 `json:"label"`
	Properties map[string]interface{} `json:"properties"`
	Edges      []Edge
}

func (n *Vertex) SetDeleted() error {
	_, err := gremlinClient.Send(
		gremlin.Query("g.V(_id).property('deleted', _deleted)").Bindings(
			gremlin.Bind{
				"_id":      n.ID,
				"_deleted": time.Now().Unix(),
			},
		),
	)
	return err
}

func (n Vertex) Exists() (exists bool, err error) {
	var (
		data []byte
		res  []bool
	)
	data, err = gremlinClient.Send(
		gremlin.Query(`g.V(_id).hasLabel(_type).hasNext()`).Bindings(
			gremlin.Bind{
				"_id":   n.ID,
				"_type": n.Type,
			},
		),
	)
	if err != nil {
		return exists, err
	}
	json.Unmarshal(data, &res)
	return res[0], err
}

func (n Vertex) Create() error {
	if n.Type == "" {
		return errors.New("Vertex has no type, skip.")
	}
	props, bindings := propertiesQuery(n.Properties)
	bindings["_id"] = n.ID
	bindings["_type"] = n.Type
	query := `g.addV(_type).property(id, _id)` + props + `.iterate()`
	_, err := gremlinClient.Send(
		gremlin.Query(query).Bindings(bindings),
	)
	if err != nil {
		if err == gremlin.ErrStatusInvalidRequestArguments {
			log.Errorf("Query: %s, Bindings: %s", query, bindings)
		}
		return err
	}
	return nil
}

func (n Vertex) CreateEdges() error {
	for _, edge := range n.Edges {
		err := edge.Create()
		if err != nil {
			return err
		}
	}
	return nil
}

func (n Vertex) Update() error {
	if n.Type == "" {
		return errors.New("Vertex has no type, skip.")
	}
	query := `g.V(_id).properties().drop()`
	_, err := gremlinClient.Send(
		gremlin.Query(query).Bindings(gremlin.Bind{
			"_id": n.ID,
		}),
	)
	if err != nil {
		return err
	}
	props, bindings := propertiesQuery(n.Properties)
	bindings["_id"] = n.ID
	query = `g.V(_id)` + props + `.iterate()`
	_, err = gremlinClient.Send(
		gremlin.Query(query).Bindings(bindings),
	)
	if err != nil {
		if err == gremlin.ErrStatusInvalidRequestArguments {
			log.Errorf("Query: %s, Bindings: %s", query, bindings)
		}
		return err
	}
	return nil
}

// CurrentEdges returns the Edges of the Vertex in its current state
func (n Vertex) CurrentEdges() (edges []Edge, err error) {
	var data []byte
	data, err = gremlinClient.Send(
		gremlin.Query(`g.V(_id).bothE()`).Bindings(
			gremlin.Bind{
				"_id": n.ID,
			},
		),
	)
	if err != nil {
		return nil, err
	}
	json.Unmarshal(data, &edges)

	return edges, err
}

func (n Vertex) DiffEdges() ([]Edge, []Edge, []Edge, error) {
	var (
		toAdd    []Edge
		toRemove []Edge
		toUpdate []Edge
	)

	currentEdges, err := n.CurrentEdges()
	if err != nil {
		return toAdd, toUpdate, toRemove, err
	}

	for _, l1 := range n.Edges {
		found := false
		update := false
		for _, l2 := range currentEdges {
			if l1.Source == l2.Source && l1.Target == l2.Target && l1.Type == l2.Type {
				found = true
				if len(l1.Properties) == 0 && len(l2.Properties) == 0 {
					break
				}
				if !cmp.Equal(l1.Properties, l2.Properties) {
					update = true
				}
				break
			}
		}
		if !found {
			toAdd = append(toAdd, l1)
		}
		if found && update {
			toUpdate = append(toUpdate, l1)
		}
	}

	for _, l1 := range currentEdges {
		found := false
		for _, l2 := range n.Edges {
			if l1.Source == l2.Source && l1.Target == l2.Target && l1.Type == l2.Type {
				found = true
				break
			}
		}
		if !found {
			toRemove = append(toRemove, l1)
		}
	}

	return toAdd, toUpdate, toRemove, nil
}

// UpdateEdges check the current Vertex edges in gremlin server
// and apply node.Edges accordingly
func (n Vertex) UpdateEdges() error {
	toAdd, toUpdate, toRemove, err := n.DiffEdges()
	if err != nil {
		return err
	}

	for _, edge := range toAdd {
		err = edge.Create()
		if err != nil {
			return err
		}
	}

	for _, edge := range toUpdate {
		err = edge.Update()
		if err != nil {
			return err
		}
	}

	for _, edge := range toRemove {
		err = edge.Delete()
		if err != nil {
			return err
		}
	}

	return nil
}

func (n Vertex) Delete() error {
	_, err := gremlinClient.Send(
		gremlin.Query(`g.V(_id).drop()`).Bindings(
			gremlin.Bind{
				"_id": n.ID,
			},
		),
	)
	if err != nil {
		return err
	}
	return nil
}

func (n Vertex) AddProperties(prefix string, c *gabs.Container) {

	if _, ok := noFlatten[prefix]; ok {
		n.AddProperty(prefix, c.String())
		return
	}

	if _, ok := c.Data().([]interface{}); ok {
		childs, _ := c.Children()
		for i, child := range childs {
			n.AddProperties(fmt.Sprintf("%s.%d", prefix, i), child)
		}
		return
	}
	if _, ok := c.Data().(map[string]interface{}); ok {
		childs, _ := c.ChildrenMap()
		for key, child := range childs {
			n.AddProperties(fmt.Sprintf("%s.%s", prefix, key), child)
		}
		return
	}
	if str, ok := c.Data().(string); ok {
		n.AddProperty(prefix, str)
		return
	}
	if boul, ok := c.Data().(bool); ok {
		n.AddProperty(prefix, boul)
		return
	}
	if num, ok := c.Data().(json.Number); ok {
		if num, err := num.Int64(); err == nil {
			n.AddProperty(prefix, num)
			return
		}
		if num, err := num.Float64(); err == nil {
			n.AddProperty(prefix, num)
			return
		}
	}
	n.AddProperty(prefix, "null")
}

func (n Vertex) AddProperty(prefix string, value interface{}) {
	if _, ok := n.Properties[prefix]; !ok {
		n.Properties[prefix] = value
	}
}

func setupRabbit(rabbitURI string, rabbitVHost string, rabbitQueue string) (*amqp.Connection, *amqp.Channel, <-chan amqp.Delivery) {
	log.Notice("Connecting to RabbitMQ...")

	conn, err := amqp.DialConfig(rabbitURI, amqp.Config{Vhost: rabbitVHost})
	if err != nil {
		log.Fatalf("Failed to connect: %s", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open channel: %s", err)
	}

	q, err := ch.QueueDeclare(
		rabbitQueue, // name
		false,       // durable
		false,       // delete when unused
		true,        // exclusive
		false,       // no-wait
		amqp.Table{"x-expires": int32(180000)}, // arguments
	)
	if err != nil {
		log.Fatalf("Failed to create queue: %s", err)
	}

	err = ch.QueueBind(
		q.Name,      // queue name
		"",          // routing key
		VncExchange, // exchange
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to bind queue: %s", err)
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		log.Fatalf("Failed to register consumer: %s", err)
	}

	log.Notice("Connected.")

	return conn, ch, msgs
}

type Sync struct {
	session    gockle.Session
	msgs       <-chan amqp.Delivery
	historize  bool
	pending    chan Notification
	idle       atomic.Value
	processing atomic.Value
}

func NewSync(session gockle.Session, msgs <-chan amqp.Delivery, historize bool) *Sync {
	s := &Sync{
		session:   session,
		msgs:      msgs,
		historize: historize,
	}
	s.idle.Store(true)
	s.processing.Store(false)
	return s
}

func (s *Sync) setupGremlin(gremlinURI string) {
	log.Notice("Connecting to Gremlin server...")
	s.listenPendingNotifications()

	gremlinClient = gremlin.NewClient(gremlinURI)
	gremlinClient.AddConnectedHandler(s.connectHandler)
	gremlinClient.AddDisconnectedHandler(s.disconnectHandler)
	gremlinClient.Connect()
}

func (s *Sync) connectHandler() {
	log.Notice("Connected to Gremlin server.")
	s.processing.Store(true)
	s.idle.Store(false)
	close(s.pending)
}

func (s *Sync) disconnectHandler(err error) {
	log.Errorf("Disconnected from Gremlin server: %s", err)
	s.idle.Store(true)
}

func (s *Sync) synchronize() {
	for d := range s.msgs {
		n := Notification{}
		json.Unmarshal(d.Body, &n)
		// Wait pending processing to finish before handling
		// new notifications
		for s.processing.Load() == true {
			time.Sleep(500 * time.Millisecond)
		}
		if s.idle.Load() == true {
			s.handlePendingNotification(n)
			d.Ack(false)
		} else {
			if s.handleNotification(n) {
				d.Ack(false)
			} else {
				d.Nack(false, false)
			}
		}
	}
}

func (s *Sync) listenPendingNotifications() {
	s.pending = make(chan Notification, 100)
	go s.addPendingNotifications()
}

func (s *Sync) handlePendingNotification(n Notification) {
	s.pending <- n
}

func (s *Sync) removePendingNotification(p []Notification, n Notification, i int) []Notification {
	log.Debugf("[%s] %s/%s [-]", n.Oper, n.Type, n.UUID)
	return append(p[:i], p[i+1:]...)
}

func (s *Sync) addPendingNotifications() {
	var pending []Notification
	for n := range s.pending {
		switch n.Oper {
		// On DELETE, remove previous notifications in the pending list
		case "DELETE":
			for i := 0; i < len(pending); i++ {
				n2 := pending[i]
				if n2.UUID == n.UUID {
					pending = s.removePendingNotification(pending, n2, i)
					i--
				}
			}
		// Reduce resource updates
		case "UPDATE":
			for i := 0; i < len(pending); i++ {
				n2 := pending[i]
				if n2.UUID == n.UUID && n2.Oper == n.Oper {
					pending = s.removePendingNotification(pending, n2, i)
					i--
				}
			}
		}
		pending = append(pending, n)
		log.Debugf("[%s] %s/%s [+]", n.Oper, n.Type, n.UUID)
	}

	s.processPendingNotifications(pending)
}

func (s *Sync) processPendingNotifications(pending []Notification) {
	log.Debugf("Processing pending notifications...")
	for _, n := range pending {
		s.handleNotification(n)
	}
	log.Debugf("Done.")
	s.processing.Store(false)
	s.listenPendingNotifications()
}

func (s Sync) handleNotificationError(n Notification, err error) bool {
	log.Errorf("[%s] %s/%s failed: %s", n.Oper, n.Type, n.UUID, err)
	switch err {
	case gremlin.ErrConnectionClosed:
		s.handlePendingNotification(n)
	}
	return false
}

func (s Sync) handleNotification(n Notification) bool {
	log.Debugf("[%s] %s/%s", n.Oper, n.Type, n.UUID)
	switch n.Oper {
	case "CREATE":
		node, err := getContrailResource(s.session, n.UUID)
		if err != nil {
			return s.handleNotificationError(n, err)
		}
		err = node.Create()
		if err != nil {
			return s.handleNotificationError(n, err)
		}
		err = node.CreateEdges()
		if err != nil {
			return s.handleNotificationError(n, err)
		}
		return true
	case "UPDATE":
		node, err := getContrailResource(s.session, n.UUID)
		if err != nil {
			return s.handleNotificationError(n, err)
		}
		err = node.Update()
		if err != nil {
			return s.handleNotificationError(n, err)
		}
		err = node.UpdateEdges()
		if err != nil {
			return s.handleNotificationError(n, err)
		}
		return true
	case "DELETE":
		node := Vertex{ID: n.UUID}
		var err error
		if s.historize {
			err = node.SetDeleted()
		} else {
			err = node.Delete()
		}
		if err != nil {
			return s.handleNotificationError(n, err)
		}
		return true
	default:
		log.Errorf("Notification not handled: %s", n)
		return false
	}
}

func getContrailResource(session gockle.Session, uuid string) (Vertex, error) {
	var (
		column1   string
		valueJSON []byte
	)
	rows, err := session.ScanMapSlice(`SELECT key, column1, value FROM obj_uuid_table WHERE key=?`, uuid)
	if err != nil {
		log.Errorf("[%s] %s", uuid, err)
		return Vertex{}, err
	}
	node := Vertex{
		ID:         uuid,
		Properties: map[string]interface{}{},
	}
	for _, row := range rows {
		column1 = string(row["column1"].([]byte))
		valueJSON = []byte(row["value"].(string))
		split := strings.Split(column1, ":")
		switch split[0] {
		case "parent", "ref":
			edge := Edge{
				Source:     uuid,
				Target:     split[2],
				TargetType: split[1],
				Type:       split[0],
			}
			if len(valueJSON) > 0 {
				edge.Properties = make(map[string]interface{})
				value, err := parseJSON(valueJSON)
				if err != nil {
					log.Errorf("Failed to parse %v", string(valueJSON))
				} else {
					edge.AddProperties("", value)
				}
			}
			node.Edges = append(node.Edges, edge)
		case "backref", "children":
			var label string
			if split[0] == "backref" {
				label = "ref"
			} else {
				label = "parent"
			}
			edge := Edge{
				Source:     split[2],
				SourceType: split[1],
				Target:     uuid,
				Type:       label,
			}
			if len(valueJSON) > 0 {
				edge.Properties = make(map[string]interface{})
				value, err := parseJSON(valueJSON)
				if err != nil {
					log.Errorf("Failed to parse %v", string(valueJSON))
				} else {
					edge.AddProperties("", value)
				}
			}
			node.Edges = append(node.Edges, edge)
		case "type":
			var value string
			json.Unmarshal(valueJSON, &value)
			node.Type = value
		case "fq_name":
			var value []string
			json.Unmarshal(valueJSON, &value)
			node.AddProperty("fq_name", value)
		case "prop":
			value, err := parseJSON(valueJSON)
			if err != nil {
				log.Errorf("Failed to parse %v", string(valueJSON))
			} else {
				node.AddProperties(split[1], value)
			}
		}
	}

	if created, ok := node.Properties["id_perms.created"]; ok {
		if time, err := time.Parse(time.RFC3339Nano, created.(string)+`Z`); err == nil {
			node.AddProperty("created", time.Unix())
		}
	}
	if updated, ok := node.Properties["id_perms.last_modified"]; ok {
		if time, err := time.Parse(time.RFC3339Nano, updated.(string)+`Z`); err == nil {
			node.AddProperty("updated", time.Unix())
		}
	}

	node.AddProperty("deleted", 0)

	return node, nil
}

func setupCassandra(cassandraCluster []string) (gockle.Session, error) {
	log.Notice("Connecting to Cassandra...")
	cluster := gocql.NewCluster(cassandraCluster...)
	cluster.Keyspace = "config_db_uuid"
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = 2000 * time.Millisecond
	cluster.DisableInitialHostLookup = true
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	mockableSession := gockle.NewSession(session)
	log.Notice("Connected.")
	return mockableSession, err
}

func setup(gremlinURI string, cassandraCluster []string, rabbitURI string, rabbitVHost string, rabbitQueue string, historize bool) {
	var (
		conn    *amqp.Connection
		msgs    <-chan amqp.Delivery
		session gockle.Session
		err     error
	)

	backend := logging.NewLogBackend(os.Stderr, "", 0)
	backendFormatter := logging.NewBackendFormatter(backend, format)
	logging.SetBackend(backendFormatter)

	session, err = setupCassandra(cassandraCluster)
	if err != nil {
		log.Fatalf("Failed to connect to Cassandra: %s", err)
	}
	defer session.Close()

	conn, _, msgs = setupRabbit(rabbitURI, rabbitVHost, rabbitQueue)
	defer conn.Close()

	sync := NewSync(session, msgs, historize)

	go sync.setupGremlin(gremlinURI)
	go sync.synchronize()

	log.Notice("Listening for updates.")
	log.Notice("To exit press CTRL+C")
	forever := make(chan bool)
	<-forever
}

func main() {
	app := cli.App("gremlin-loader", "Load and Sync Contrail DB in Gremlin Server")
	gremlinSrv := app.String(cli.StringOpt{
		Name:   "gremlin",
		Value:  "localhost:8182",
		Desc:   "host:port of gremlin server",
		EnvVar: "GREMLIN_SYNC_GREMLIN_SERVER",
	})
	cassandraSrvs := app.Strings(cli.StringsOpt{
		Name:   "cassandra",
		Value:  []string{"localhost"},
		Desc:   "list of host of cassandra nodes, uses CQL port 9042",
		EnvVar: "GREMLIN_SYNC_CASSANDRA_SERVERS",
	})
	rabbitSrv := app.String(cli.StringOpt{
		Name:   "rabbit",
		Value:  "localhost:5672",
		Desc:   "host:port of rabbitmq server",
		EnvVar: "GREMLIN_SYNC_RABBIT_SERVER",
	})
	rabbitVHost := app.String(cli.StringOpt{
		Name:   "rabbit-vhost",
		Value:  "opencontrail",
		Desc:   "vhost of rabbitmq server",
		EnvVar: "GREMLIN_SYNC_RABBIT_VHOST",
	})
	rabbitUser := app.String(cli.StringOpt{
		Name:   "rabbit-user",
		Value:  "opencontrail",
		Desc:   "user for rabbitmq server",
		EnvVar: "GREMLIN_SYNC_RABBIT_USER",
	})
	rabbitPassword := app.String(cli.StringOpt{
		Name:   "rabbit-password",
		Desc:   "password for rabbitmq server",
		EnvVar: "GREMLIN_SYNC_RABBIT_PASSWORD",
	})
	rabbitQueue := app.String(cli.StringOpt{
		Name:   "rabbit-queue",
		Value:  QueueName,
		Desc:   "name of rabbitmq name",
		EnvVar: "GREMLIN_SYNC_RABBIT_QUEUE",
	})
	historize := app.Bool(cli.BoolOpt{
		Name:   "historize",
		Value:  false,
		Desc:   "Mark nodes as deleted but don't drop them",
		EnvVar: "GREMLIN_SYNC_HISTORIZE",
	})
	app.Action = func() {
		gremlinURI := fmt.Sprintf("ws://%s/gremlin", *gremlinSrv)
		rabbitURI := fmt.Sprintf("amqp://%s:%s@%s/", *rabbitUser,
			*rabbitPassword, *rabbitSrv)
		setup(gremlinURI, *cassandraSrvs, rabbitURI, *rabbitVHost,
			*rabbitQueue, *historize)
	}
	app.Run(os.Args)
}
