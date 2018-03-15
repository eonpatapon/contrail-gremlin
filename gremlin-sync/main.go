package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	g "github.com/eonpatapon/contrail-gremlin/gremlin"
	"github.com/eonpatapon/contrail-gremlin/utils"
	"github.com/eonpatapon/gremlin"
	"github.com/jawher/mow.cli"
	logging "github.com/op/go-logging"
	"github.com/satori/go.uuid"
	"github.com/streadway/amqp"
	"github.com/willfaught/gockle"
)

var (
	log = logging.MustGetLogger(os.Args[0])
	// DeleteInterval is the time to wait before checking if
	// a resource was correctly deleted from the contrail db
	DeleteInterval = 3 * time.Second
)

const (
	// VncExchange is the rabbitmq exchange for contrail
	VncExchange = "vnc_config.object-update"
	// QueueName is the rabbitmq queue for the sync process
	QueueName = "gremlin.sync"
)

// Notification represent rabbitmq notifications from contrail-api
type Notification struct {
	Oper string    `json:"oper"`
	Type string    `json:"type"`
	UUID uuid.UUID `json:"uuid"`
}

// Sync represent the state of the sync process
type Sync struct {
	backend           *g.ServerBackend
	session           gockle.Session
	msgs              <-chan amqp.Delivery
	pending           []Notification
	pendingProcessing atomic.Value
	wg                *sync.WaitGroup
}

// NewSync returns the sync process
func NewSync(session gockle.Session, msgs <-chan amqp.Delivery, gremlinURI string) *Sync {
	s := &Sync{
		backend: g.NewServerBackend(gremlinURI),
		session: session,
		msgs:    msgs,
		pending: []Notification{},
		wg:      &sync.WaitGroup{},
	}
	s.pendingProcessing.Store(false)
	s.backend.AddConnectedHandler(s.onConnected)
	s.backend.AddDisconnectedHandler(s.onDisconnected)
	return s
}

func (s *Sync) start() {
	log.Notice("Connecting to Gremlin Server...")
	s.backend.Start()
}

func (s *Sync) stop() {
	s.wg.Wait()
	s.backend.Stop()
	log.Notice("Disconnected from Gremlin Server.")
}

func (s *Sync) onConnected() {
	log.Notice("Connected to Gremlin Server")
	if len(s.pending) > 0 {
		s.pendingProcessing.Store(true)
		s.processPendingNotifications()
		s.pendingProcessing.Store(false)
	}
}

func (s *Sync) onDisconnected(err error) {
	if err != nil {
		log.Errorf("Disconnected from Gremlin Server: %s", err)
	}
}

func (s *Sync) synchronize() {
	log.Debug("Listening for updates")
	for d := range s.msgs {
		n := Notification{}
		json.Unmarshal(d.Body, &n)

		if s.backend.Connected() == false {
			s.handlePendingNotification(n)
			d.Ack(false)
			continue
		}

		for s.pendingProcessing.Load() == true {
			time.Sleep(200 * time.Millisecond)
		}

		if err := s.handleNotification(n); err != nil {
			d.Ack(false)
		} else {
			d.Nack(false, false)
		}
	}
}

func (s *Sync) handlePendingNotification(n Notification) {
	switch n.Oper {
	// On DELETE, remove previous notifications in the pending list
	case "DELETE":
		for i := 0; i < len(s.pending); i++ {
			n2 := s.pending[i]
			if n2.UUID == n.UUID {
				s.pending = s.removePendingNotification(s.pending, n2, i)
				i--
			}
		}
	// Reduce resource updates
	case "UPDATE":
		for i := 0; i < len(s.pending); i++ {
			n2 := s.pending[i]
			if n2.UUID == n.UUID && n2.Oper == n.Oper {
				s.pending = s.removePendingNotification(s.pending, n2, i)
				i--
			}
		}
	}
	s.pending = append(s.pending, n)
	log.Debugf("[%s] %s/%s [+]", n.Oper, n.Type, n.UUID)
}

func (s *Sync) removePendingNotification(p []Notification, n Notification, i int) []Notification {
	log.Debugf("[%s] %s/%s [-]", n.Oper, n.Type, n.UUID)
	return append(p[:i], p[i+1:]...)
}

func (s *Sync) processPendingNotifications() {
	log.Debugf("Processing pending notifications...")
	pending := s.pending
	for _, n := range pending {
		err := s.handleNotification(n)
		if err == gremlin.ErrConnectionClosed {
			log.Errorf("Disconnected while processing pending list.")
			return
		}
		s.pending = s.pending[1:]
	}
	log.Debugf("Done.")
}

func (s *Sync) handleNotificationError(n Notification, err error) error {
	log.Errorf("[%s] %s/%s failed: %s", n.Oper, n.Type, n.UUID, err)
	if s.pendingProcessing.Load() == false && err == gremlin.ErrConnectionClosed {
		s.handlePendingNotification(n)
	}
	return err
}

func (s *Sync) handleNotification(n Notification) error {
	log.Debugf("[%s] %s/%s", n.Oper, n.Type, n.UUID)
	switch n.Oper {
	case "CREATE":
		vertex, err := utils.GetContrailResource(s.session, n.UUID)
		if err != nil {
			return s.handleNotificationError(n, err)
		}
		err = s.backend.CreateVertex(vertex)
		if err != nil {
			return s.handleNotificationError(n, err)
		}
		return nil
	case "UPDATE":
		vertex, err := utils.GetContrailResource(s.session, n.UUID)
		if err != nil {
			return s.handleNotificationError(n, err)
		}
		err = s.backend.UpdateVertex(vertex)
		if err != nil {
			return s.handleNotificationError(n, err)
		}
		return nil
	case "DELETE":
		now := time.Now()
		vertex := g.Vertex{ID: n.UUID, Label: n.Type}
		err := s.backend.UpdateVertexProperty(vertex, "deleted", now.Unix())
		if err != nil {
			return s.handleNotificationError(n, err)
		}
		s.checkDeleteLater(vertex, n)
		return nil
	default:
		log.Errorf("Notification not handled: %s", n)
		return nil
	}
}

func (s *Sync) checkDeleteLater(v g.Vertex, n Notification) {
	go func() {
		s.wg.Add(1)
		defer s.wg.Done()
		time.Sleep(DeleteInterval)
		s.checkDelete(v, n)
	}()
}

func (s Sync) checkDelete(v g.Vertex, n Notification) error {
	cv, err := utils.GetContrailResource(s.session, v.ID)
	switch err {
	case utils.ErrResourceNotFound:
		err := s.backend.DeleteVertex(v)
		if err != nil {
			return s.handleNotificationError(n, err)
		}
	// the vertex is still present in the DB
	// but should have been deleted
	case nil:
		log.Errorf("Resource %s/%s is still present in DB", v.Label, v.ID)
		// the resource is not deleted completely
		if cv.HasProp("_incomplete") {
			cv.Label = v.Label
			cv.Properties["deleted"] = v.Properties["deleted"]
		}
		err := s.backend.UpdateVertex(cv)
		if err != nil {
			return s.handleNotificationError(n, err)
		}
	default:
		log.Errorf("Failed to retrieve resource %s from db: %s", v.ID, err)
		s.checkDeleteLater(v, n)
	}
	return nil
}

func setup(gremlinURI string, cassandraCluster []string, rabbitURI string, rabbitVHost string, rabbitQueue string) {
	var (
		conn    *amqp.Connection
		msgs    <-chan amqp.Delivery
		session gockle.Session
		err     error
	)

	log.Notice("Connecting to Cassandra...")
	session, err = utils.SetupCassandra(cassandraCluster)
	if err != nil {
		log.Fatalf("Failed to connect to Cassandra: %s", err)
	}
	log.Notice("Connected.")
	defer session.Close()

	conn, _, msgs = setupRabbit(rabbitURI, rabbitVHost, rabbitQueue)
	defer conn.Close()

	sync := NewSync(session, msgs, gremlinURI)
	go sync.synchronize()
	sync.start()
	defer sync.stop()

	log.Notice("To exit press CTRL+C")
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGKILL, syscall.SIGTERM)
	<-c
}

func main() {
	app := cli.App(os.Args[0], "Sync Contrail DB in Gremlin Server")
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
	utils.SetupLogging(app, log)
	app.Action = func() {
		gremlinURI := fmt.Sprintf("ws://%s/gremlin", *gremlinSrv)
		rabbitURI := fmt.Sprintf("amqp://%s:%s@%s/", *rabbitUser,
			*rabbitPassword, *rabbitSrv)
		setup(gremlinURI, *cassandraSrvs, rabbitURI, *rabbitVHost,
			*rabbitQueue)
	}
	app.Run(os.Args)
}
