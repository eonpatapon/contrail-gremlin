package utils

import (
	"testing"

	g "github.com/eonpatapon/contrail-gremlin/gremlin"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/willfaught/gockle"
)

func TestGetContrailResource(t *testing.T) {
	id1, _ := uuid.NewV4()
	id2, _ := uuid.NewV4()
	id3, _ := uuid.NewV4()
	query := "SELECT key, column1, value FROM obj_uuid_table WHERE key=?"

	session := &gockle.SessionMock{}
	session.When("Close").Return()
	session.When("ScanMapSlice", query, []interface{}{id1.String()}).Return(
		[]map[string]interface{}{
			{"column1": []byte("type"), "value": `"foo"`},
			{"column1": []byte("prop:integer"), "value": `12`},
			{"column1": []byte("prop:string"), "value": `"str"`},
			{"column1": []byte("prop:list"), "value": `["a", "b"]`},
			{"column1": []byte("prop:object"), "value": `{"bool": false, "sub_object": {"foo": "bar"}}`},
			{"column1": []byte("ref:bar:" + id2.String()), "value": `{"foo": false}`},
			{"column1": []byte("children:foobar:" + id3.String()), "value": `{"foo": false}`},
		},
		nil,
	)

	expectedVertex := g.Vertex{
		ID:    id1,
		Label: "foo",
	}
	expectedVertex.AddProperties(map[string]interface{}{
		"integer":               int64(12),
		"string":                "str",
		"list.0":                "a",
		"list.1":                "b",
		"object.bool":           false,
		"object.sub_object.foo": "bar",
		"_incomplete":           true,
		"deleted":               -1,
	})
	expectedVertex.AddOutEdge(g.Edge{
		Label:    "ref",
		InVLabel: "bar",
		InV:      id2,
		OutV:     id1,
		Properties: map[string]g.Property{
			"foo": g.Property{Value: false},
		},
	})
	expectedVertex.AddInEdge(g.Edge{
		Label:     "parent",
		InV:       id1,
		OutV:      id3,
		OutVLabel: "foobar",
		Properties: map[string]g.Property{
			"foo": g.Property{Value: false},
		},
	})

	vertex, _ := GetContrailResource(session, id1)

	assert.Equal(t, expectedVertex, vertex, "")
}

func TestGetContrailResourceNoEdges(t *testing.T) {
	id1, _ := uuid.NewV4()
	query := "SELECT key, column1, value FROM obj_uuid_table WHERE key=?"

	session := &gockle.SessionMock{}
	session.When("Close").Return()
	session.When("ScanMapSlice", query, []interface{}{id1.String()}).Return(
		[]map[string]interface{}{
			{"column1": []byte("type"), "value": `"foo"`},
		},
		nil,
	)

	expectedVertex := g.Vertex{
		ID:    id1,
		Label: "foo",
	}
	expectedVertex.AddProperty("_incomplete", true)
	expectedVertex.AddProperty("deleted", -1)

	vertex, _ := GetContrailResource(session, id1)

	assert.Equal(t, expectedVertex, vertex, "")
}
