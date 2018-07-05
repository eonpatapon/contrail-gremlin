package gremlin

import (
	"bytes"
	"io"
	"testing"

	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestNewGsonVertex(t *testing.T) {
	id, _ := uuid.NewV4()
	v := Vertex{
		ID:         id,
		Label:      "foo",
		Properties: make(map[string][]Property),
		InE:        make(map[string][]Edge),
	}
	v.AddProperty("prop1", 1)
	v.AddProperty("prop1", 3.4958)
	v.AddProperty("prop2", "bar")
	v.AddProperty("prop3", map[string]interface{}{
		"big": map[string]interface{}{
			"long": 397437162835365200,
		},
	})
	v.AddProperty("prop4", []interface{}{5, "foo"})

	_, w := io.Pipe()
	b := NewGsonBackend(w)
	gv := b.newGsonVertex(v)

	assert.Equal(t, 2, len(gv.Properties["prop1"]), "")
	assert.Equal(t, "g:Int64", gv.Properties["prop1"][0].Value.(GsonValue).Type, "")
	assert.Equal(t, "g:Float64", gv.Properties["prop1"][1].Value.(GsonValue).Type, "")
	assert.Equal(t, "bar", gv.Properties["prop2"][0].Value.(string), "")
	assert.Equal(t, "g:Map", gv.Properties["prop3"][0].Value.(GsonValue).Type, "")
	assert.Equal(t, []interface{}{"big", GsonValue{Type: "g:Map", Value: []interface{}{"long", GsonValue{Type: "g:Int64", Value: int64(397437162835365200)}}}}, gv.Properties["prop3"][0].Value.(GsonValue).Value.([]interface{}), "")
	assert.Equal(t, "g:List", gv.Properties["prop4"][0].Value.(GsonValue).Type, "")
	assert.Equal(t, []interface{}{GsonValue{Type: "g:Int64", Value: int64(5)}, "foo"}, gv.Properties["prop4"][0].Value.(GsonValue).Value.([]interface{}), "")
}

func TestEdgeIDs(t *testing.T) {
	_, w := io.Pipe()
	b := NewGsonBackend(w)

	id1, _ := uuid.NewV4()
	v1 := Vertex{
		ID:   id1,
		OutE: make(map[string][]Edge),
	}
	id2, _ := uuid.NewV4()
	v2 := Vertex{
		ID:  id2,
		InE: make(map[string][]Edge),
	}

	e1 := Edge{
		InV:   id2,
		Label: "foo",
	}
	v1.AddOutEdge(e1)
	e2 := Edge{
		OutV:  id1,
		Label: "foo",
	}
	v2.AddInEdge(e2)

	gv1 := b.newGsonVertex(v1)
	gv2 := b.newGsonVertex(v2)

	assert.Equal(t, gv1.OutE["foo"][0].InV.Value, gv2.ID.Value, "")
	assert.Equal(t, gv1.ID.Value, gv2.InE["foo"][0].OutV.Value, "")
	assert.Equal(t, gv1.OutE["foo"][0].ID.Value.(int64),
		gv2.InE["foo"][0].ID.Value.(int64), "")
}

func TestMultipleEdges(t *testing.T) {
	id1, _ := uuid.NewV4()
	id2, _ := uuid.NewV4()
	id3, _ := uuid.NewV4()
	id4, _ := uuid.NewV4()
	id5, _ := uuid.NewV4()
	v1 := Vertex{
		ID:   id1,
		OutE: make(map[string][]Edge),
		InE:  make(map[string][]Edge),
	}
	e1 := Edge{
		InV:   id2,
		Label: "foo",
	}
	e2 := Edge{
		InV:   id3,
		Label: "foo",
	}
	e3 := Edge{
		OutV:  id4,
		Label: "foo",
	}
	e4 := Edge{
		OutV:  id5,
		Label: "foo",
	}
	v1.AddOutEdge(e1)
	v1.AddOutEdge(e2)
	v1.AddInEdge(e3)
	v1.AddInEdge(e4)

	_, w := io.Pipe()
	b := NewGsonBackend(w)
	b.newGsonVertex(v1)
}

func TestWrite(t *testing.T) {
	var data []byte
	buf := bytes.NewBuffer(data)
	b := NewGsonBackend(buf)
	b.Start()

	id1, _ := uuid.NewV4()
	v1 := Vertex{
		ID:    id1,
		Label: "foo",
	}

	id2, _ := uuid.NewV4()
	v2 := Vertex{
		ID:    id2,
		Label: "bar",
	}

	b.Create(v1)
	b.Create(v2)
	b.Stop()

	b2 := NewGsonBackend(buf)
	gv1 := b2.newGsonVertex(v1)
	gv2 := b2.newGsonVertex(v2)
	vJSON1, _ := gv1.toJSON()
	vJSON2, _ := gv2.toJSON()

	assert.Equal(t, string(vJSON1)+"\n"+string(vJSON2)+"\n", buf.String())
}

func TestEdgeNullProperty(t *testing.T) {
	id1, _ := uuid.NewV4()
	v1 := Vertex{
		ID:    id1,
		Label: "foo",
		OutE:  make(map[string][]Edge),
	}
	e1 := Edge{
		Label: "ref",
	}
	e1.AddProperty("prop1", nil)
	v1.AddOutEdge(e1)

	_, w := io.Pipe()
	b := NewGsonBackend(w)
	gv1 := b.newGsonVertex(v1)
	_, ok := gv1.OutE["ref"][0].Properties["prop1"]
	assert.Equal(t, false, ok)
}

func TestPendingWrite(t *testing.T) {
	var data []byte
	buf := bytes.NewBuffer(data)
	b := NewGsonBackend(buf)
	b.Start()

	id1, _ := uuid.NewV4()
	v1 := Vertex{
		ID:    id1,
		Label: "foo",
		OutE:  make(map[string][]Edge),
	}
	id2, _ := uuid.NewV4()
	e1 := Edge{
		Label:      "ref",
		InV:        id2,
		InVLabel:   "bar",
		Properties: make(map[string]Property),
	}
	e1.AddProperty("prop1", 1)
	v1.AddOutEdge(e1)
	b.Create(v1)
	b.Stop()

	buf.ReadBytes('\n')
	vJSON2, _ := buf.ReadBytes('\n')

	gv2 := GsonVertex{}
	gv2.fromJSON(vJSON2)

	assert.Equal(t, gv2.ID.Value.(uuid.UUID), id2)
	assert.Equal(t, gv2.Properties["fq_name"][0].Value.([]interface{}),
		[]interface{}{"_missing"})
}

func TestJSON(t *testing.T) {
	id1, _ := uuid.NewV4()
	v1 := Vertex{
		ID:    id1,
		Label: "foo",
	}
	v1.AddProperty("prop1", 1)
	id2, _ := uuid.NewV4()
	e1 := Edge{
		Label:    "ref",
		InV:      id2,
		InVLabel: "bar",
	}
	e1.AddProperty("prop2", 1)
	v1.AddOutEdge(e1)

	_, w := io.Pipe()
	b := NewGsonBackend(w)

	gv1 := b.newGsonVertex(v1)

	gv1JSON, _ := gv1.toJSON()
	gv2 := GsonVertex{}
	gv2.fromJSON(gv1JSON)

	assert.Equal(t, gv1, gv2)
}
