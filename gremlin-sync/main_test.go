package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/eonpatapon/gremlin"
	uuid "github.com/satori/go.uuid"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"github.com/willfaught/gockle"
)

var gremlinURI = "ws://localhost:8182/gremlin"

func init() {
	fakeChan := make(<-chan amqp.Delivery)
	sync := NewSync(gockle.SessionMock{}, fakeChan, false)
	sync.setupGremlin(gremlinURI)
}

func checkNode(t *testing.T, query string, bindings gremlin.Bind) []string {
	var uuids []string
	results, err := gremlinClient.Send(
		gremlin.Query(query).Bindings(bindings),
	)
	if err != nil {
		t.Errorf("Failed to run query: %s (%s)", query, err)
		t.SkipNow()
	}
	json.Unmarshal(results, &uuids)
	return uuids
}

func TestNodeLink(t *testing.T) {
	var uuids []string

	vnUUID, _ := uuid.NewV4()
	vn := Vertex{
		ID:   vnUUID.String(),
		Type: "virtual_machine",
	}
	vn.Create()

	vmiUUID, _ := uuid.NewV4()
	vmi := Vertex{
		ID:   vmiUUID.String(),
		Type: "virtual_machine_interface",
		Edges: []Edge{
			Edge{
				Source: vmiUUID.String(),
				Target: vnUUID.String(),
				Type:   "ref",
			},
		},
	}
	vmi.Create()
	vmi.CreateEdges()

	uuids = checkNode(t, `g.V(uuid).in('ref').id()`, gremlin.Bind{"uuid": vnUUID})

	assert.Equal(t, 1, len(uuids), "One resource must be linked")
	assert.Equal(t, vmiUUID.String(), uuids[0], "VMI not correctly linked to VN")

	projectUUID, _ := uuid.NewV4()
	project := Vertex{
		ID:   projectUUID.String(),
		Type: "project",
	}
	project.Create()

	vmi.Edges = append(vmi.Edges, Edge{
		Source: projectUUID.String(),
		Target: vmiUUID.String(),
		Type:   "parent",
	})
	vmi.UpdateEdges()

	uuids = checkNode(t, `g.V(uuid).both().id()`, gremlin.Bind{"uuid": vmiUUID.String()})

	assert.Equal(t, 2, len(uuids), "Two resources must be linked")
}

func TestNodeProperties(t *testing.T) {
	nodeUUID, _ := uuid.NewV4()
	nodeUUIDStr := nodeUUID.String()
	query := "SELECT key, column1, value FROM obj_uuid_table WHERE key=?"

	session := &gockle.SessionMock{}
	session.When("Close").Return()
	session.When("ScanMapSlice", query, []interface{}{nodeUUIDStr}).Return(
		[]map[string]interface{}{
			{"column1": []byte("type"), "value": `"virtual_machine"`},
			{"column1": []byte("prop:integer"), "value": `12`},
			{"column1": []byte("prop:string"), "value": `"str"`},
			{"column1": []byte("prop:list"), "value": `["a", "b", "c"]`},
			{"column1": []byte("prop:object"), "value": `{"bool": false, "subObject": {"foo": "bar"}}`},
		},
		nil,
	)

	node, _ := getContrailResource(session, nodeUUIDStr)
	node.Create()

	var uuids []string

	uuids = checkNode(t, `g.V(uuid).has('integer', 12).id()`, gremlin.Bind{"uuid": nodeUUIDStr})
	assert.Equal(t, nodeUUIDStr, uuids[0])
	uuids = checkNode(t, `g.V(uuid).has('string', 'str').id()`, gremlin.Bind{"uuid": nodeUUIDStr})
	assert.Equal(t, nodeUUIDStr, uuids[0])
	uuids = checkNode(t, `g.V(uuid).has('list.0', 'a').id()`, gremlin.Bind{"uuid": nodeUUIDStr})
	assert.Equal(t, nodeUUIDStr, uuids[0])
	uuids = checkNode(t, `g.V(uuid).has('object.bool', false).id()`, gremlin.Bind{"uuid": nodeUUIDStr})
	assert.Equal(t, nodeUUIDStr, uuids[0])
	uuids = checkNode(t, `g.V(uuid).has('object.subObject.foo', 'bar').id()`, gremlin.Bind{"uuid": nodeUUIDStr})
	assert.Equal(t, nodeUUIDStr, uuids[0])
}

func TestNodeExists(t *testing.T) {
	nodeUUID, _ := uuid.NewV4()
	node := Vertex{
		ID:   nodeUUID.String(),
		Type: "label",
		Properties: map[string]interface{}{
			"prop": "value",
		},
	}
	node.Create()
	exists, _ := node.Exists()
	assert.Equal(t, true, exists)

	nodeUUID, _ = uuid.NewV4()
	node.ID = nodeUUID.String()
	exists, _ = node.Exists()
	assert.Equal(t, false, exists)
}

func TestEdgeExists(t *testing.T) {
	node1UUID, _ := uuid.NewV4()
	node1 := Vertex{
		ID:   node1UUID.String(),
		Type: "foo",
	}
	node1.Create()

	node2UUID, _ := uuid.NewV4()
	node2 := Vertex{
		ID:   node2UUID.String(),
		Type: "bar",
	}
	node2.Create()

	link := Edge{
		Source: node1UUID.String(),
		Target: node2UUID.String(),
		Type:   "foobar",
	}
	exists, _ := link.Exists()
	assert.Equal(t, exists, false)
	link.Create()
	exists, _ = link.Exists()
	assert.Equal(t, exists, true)
}

func TestEdgeProperties(t *testing.T) {
	node1UUID, _ := uuid.NewV4()
	node1 := Vertex{
		ID:   node1UUID.String(),
		Type: "foo",
	}
	node1.Create()

	node2UUID, _ := uuid.NewV4()
	node2 := Vertex{
		ID:   node2UUID.String(),
		Type: "bar",
	}
	node2.Create()

	link := Edge{
		Source: node1UUID.String(),
		Target: node2UUID.String(),
		Type:   "foobar",
		Properties: map[string]interface{}{
			"foo": "bar",
		},
	}
	link.Create()

	var (
		data  []byte
		links []Edge
	)
	data, _ = gremlinClient.Send(
		gremlin.Query(`g.V(_id).bothE()`).Bindings(
			gremlin.Bind{
				"_id": node1UUID,
			},
		),
	)
	json.Unmarshal(data, &links)

	assert.Equal(t, len(links), 1)
	assert.Equal(t, links[0].Properties["foo"].(string), "bar")

	link = Edge{
		Source: node1UUID.String(),
		Target: node2UUID.String(),
		Type:   "foobar",
		Properties: map[string]interface{}{
			"foo": "barbar",
		},
	}
	link.Update()

	data, _ = gremlinClient.Send(
		gremlin.Query(`g.V(_id).bothE()`).Bindings(
			gremlin.Bind{
				"_id": node1UUID,
			},
		),
	)
	json.Unmarshal(data, &links)

	assert.Equal(t, links[0].Properties["foo"].(string), "barbar")
}

func TestEdgeDiff(t *testing.T) {
	node1UUID, _ := uuid.NewV4()
	node1 := Vertex{
		ID:   node1UUID.String(),
		Type: "foo",
	}
	node1.Create()

	node2UUID, _ := uuid.NewV4()
	node2 := Vertex{
		ID:   node2UUID.String(),
		Type: "bar",
		Edges: []Edge{
			Edge{
				Source: node2UUID.String(),
				Target: node1UUID.String(),
				Type:   "ref",
			},
		},
	}
	node2.Create()

	toAdd, toUpdate, toRemove, _ := node2.DiffEdges()
	assert.Equal(t, 1, len(toAdd))
	assert.Equal(t, 0, len(toRemove))

	node2.CreateEdges()

	toAdd, _, toRemove, _ = node2.DiffEdges()
	assert.Equal(t, 0, len(toAdd))
	assert.Equal(t, 0, len(toRemove))

	node2b := Vertex{
		ID:   node2UUID.String(),
		Type: "bar",
		Edges: []Edge{
			Edge{
				Source: node2UUID.String(),
				Target: node1UUID.String(),
				Type:   "parent",
			},
		},
	}
	toAdd, _, toRemove, _ = node2b.DiffEdges()
	assert.Equal(t, 1, len(toAdd))
	assert.Equal(t, 1, len(toRemove))

	node2c := Vertex{
		ID:   node2UUID.String(),
		Type: "bar",
		Edges: []Edge{
			Edge{
				Source: node2UUID.String(),
				Target: node1UUID.String(),
				Type:   "ref",
				Properties: map[string]interface{}{
					"foo": "bar",
				},
			},
		},
	}

	toAdd, toUpdate, toRemove, _ = node2c.DiffEdges()
	assert.Equal(t, 0, len(toAdd))
	assert.Equal(t, 1, len(toUpdate))
	assert.Equal(t, 0, len(toRemove))

}

func TestSynchro(t *testing.T) {
	nodeUUID, _ := uuid.NewV4()

	query := "SELECT key, column1, value FROM obj_uuid_table WHERE key=?"
	session := &gockle.SessionMock{}
	session.When("Close").Return()
	session.When("ScanMapSlice", query, []interface{}{nodeUUID.String()}).Return(
		[]map[string]interface{}{
			{"column1": []byte("type"), "value": `"virtual_machine"`},
			{"column1": []byte("fq_name"), "value": `["foo"]`},
		},
		nil,
	)

	msgs := make(chan amqp.Delivery)

	sync := NewSync(session, msgs, false)

	go sync.setupGremlin(gremlinURI)
	go sync.synchronize()

	time.Sleep(1 * time.Second)

	msgs <- amqp.Delivery{Body: []byte(fmt.Sprintf(`{"oper": "CREATE", "type": "virtual_machine", "uuid": "%s"}`, nodeUUID.String()))}

	time.Sleep(1 * time.Second)

	uuids := checkNode(t, `g.V(uuid).hasLabel("virtual_machine").id()`, gremlin.Bind{"uuid": nodeUUID.String()})
	assert.Equal(t, nodeUUID.String(), uuids[0])
}

func TestSynchroOrdering(t *testing.T) {
	nodeUUID, _ := uuid.NewV4()

	query := "SELECT key, column1, value FROM obj_uuid_table WHERE key=?"
	session := &gockle.SessionMock{}
	session.When("Close").Return()
	session.When("ScanMapSlice", query, []interface{}{nodeUUID.String()}).Return(
		[]map[string]interface{}{
			{"column1": []byte("type"), "value": `"virtual_machine"`},
			{"column1": []byte("fq_name"), "value": `["foo"]`},
		},
		nil,
	)

	msgs := make(chan amqp.Delivery)

	sync := NewSync(session, msgs, false)

	go sync.setupGremlin(gremlinURI)
	go sync.synchronize()

	time.Sleep(1 * time.Second)

	sync.disconnectHandler(errors.New("Fake disconnection"))
	msgs <- amqp.Delivery{Body: []byte(fmt.Sprintf(`{"oper": "CREATE", "type": "virtual_machine", "uuid": "%s"}`, nodeUUID.String()))}

	time.Sleep(500 * time.Millisecond)

	sync.connectHandler()
	msgs <- amqp.Delivery{Body: []byte(fmt.Sprintf(`{"oper": "DELETE", "type": "virtual_machine", "uuid": "%s"}`, nodeUUID.String()))}

	time.Sleep(2 * time.Second)

	var expect []string
	r := checkNode(t, `g.V(uuid).hasLabel("virtual_machine").id()`, gremlin.Bind{"uuid": nodeUUID.String()})
	assert.Equal(t, expect, r)
}
