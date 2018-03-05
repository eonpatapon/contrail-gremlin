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

	sync := NewSync(session, msgs, gremlinURI)
	go sync.synchronize()
	sync.start()

	time.Sleep(1 * time.Second)

	msgs <- amqp.Delivery{
		Body: []byte(fmt.Sprintf(`{"oper": "CREATE", "type": "virtual_machine", "uuid": "%s"}`, nodeUUID.String()))}

	time.Sleep(100 * time.Millisecond)

	var uuids []string
	r, _ := sync.backend.Send(
		gremlin.Query(`g.V(uuid).hasLabel("virtual_machine").id()`).Bindings(
			gremlin.Bind{"uuid": nodeUUID.String()},
		),
	)
	json.Unmarshal(r, &uuids)
	assert.Equal(t, []string{nodeUUID.String()}, uuids)

	sync.stop()
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

	sync := NewSync(session, msgs, gremlinURI)
	go sync.synchronize()
	sync.start()

	time.Sleep(200 * time.Millisecond)

	sync.onDisconnected(errors.New("Fake disconnection"))

	msgs <- amqp.Delivery{
		Body: []byte(fmt.Sprintf(`{"oper": "CREATE", "type": "virtual_machine", "uuid": "%s"}`, nodeUUID))}
	msgs <- amqp.Delivery{
		Body: []byte(fmt.Sprintf(`{"oper": "UPDATE", "type": "virtual_machine", "uuid": "%s"}`, nodeUUID))}
	msgs <- amqp.Delivery{
		Body: []byte(fmt.Sprintf(`{"oper": "UPDATE", "type": "virtual_machine", "uuid": "%s"}`, nodeUUID))}

	time.Sleep(200 * time.Millisecond)

	sync.onConnected()

	msgs <- amqp.Delivery{
		Body: []byte(fmt.Sprintf(`{"oper": "DELETE", "type": "virtual_machine", "uuid": "%s"}`, nodeUUID))}

	sync.onDisconnected(errors.New("Fake disconnection"))

	time.Sleep(100 * time.Millisecond)

	sync.onConnected()

	var uuids []string
	r, _ := sync.backend.Send(
		gremlin.Query(`g.V(uuid).has("deleted", 0).id()`).Bindings(
			gremlin.Bind{"uuid": nodeUUID.String()},
		),
	)
	json.Unmarshal(r, &uuids)
	assert.Equal(t, 0, len(uuids))

	sync.stop()
}

func TestDelete(t *testing.T) {
	nodeUUID, _ := uuid.NewV4()
	resource := []map[string]interface{}{
		{"column1": []byte("type"), "value": `"virtual_machine"`},
		{"column1": []byte("fq_name"), "value": `["foo"]`},
		{"column1": []byte("prop:id_perms"), "value": `{"created": "2018-03-05T06:21:57.186987"}`},
	}

	query := "SELECT key, column1, value FROM obj_uuid_table WHERE key=?"
	session := &gockle.SessionMock{}
	session.When("Close").Return()
	mock := session.When("ScanMapSlice", query, []interface{}{nodeUUID.String()})
	mock.Return(resource, nil)

	msgs := make(chan amqp.Delivery)

	sync := NewSync(session, msgs, gremlinURI)
	go sync.synchronize()
	sync.start()

	time.Sleep(200 * time.Millisecond)

	msgs <- amqp.Delivery{
		Body: []byte(fmt.Sprintf(`{"oper": "CREATE", "type": "virtual_machine", "uuid": "%s"}`, nodeUUID))}

	time.Sleep(10 * time.Millisecond)

	mock.ReturnValues = []interface{}{}
	mock.Return([]map[string]interface{}{}, nil)

	msgs <- amqp.Delivery{
		Body: []byte(fmt.Sprintf(`{"oper": "DELETE", "type": "virtual_machine", "uuid": "%s"}`, nodeUUID))}

	time.Sleep(DeleteInterval + 10*time.Millisecond)

	var uuids []string
	r, _ := sync.backend.Send(
		gremlin.Query(`g.V(uuid).id()`).Bindings(
			gremlin.Bind{"uuid": nodeUUID.String()},
		),
	)
	json.Unmarshal(r, &uuids)
	assert.Equal(t, 0, len(uuids))

	sync.stop()
}
