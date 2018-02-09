package gremlin

import (
	"encoding/json"
	"testing"

	"github.com/eonpatapon/gremlin"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestVertexLink(t *testing.T) {
	b := NewServerBackend("ws://127.0.0.1:8182/gremlin")
	b.Start()

	id1, _ := uuid.NewV4()
	v1 := Vertex{
		ID:    id1,
		Label: "foo",
	}
	b.CreateVertex(v1)

	id2, _ := uuid.NewV4()
	v2 := Vertex{
		ID:    id2,
		Label: "bar",
	}
	e2 := Edge{
		Label: "ref",
		InV:   v1.ID,
		OutV:  v2.ID,
	}
	v2.AddInEdge(e2)
	b.CreateVertex(v2)

	var uuids []string
	r, _ := b.Send(
		gremlin.Query(`g.V(id1).in().id()`).Bindings(gremlin.Bind{"id1": id1}),
	)
	json.Unmarshal(r, &uuids)
	assert.Equal(t, []string{id2.String()}, uuids, "")

	v2 = Vertex{
		ID:    id2,
		Label: "bar",
	}
	b.UpdateVertex(v2)

	uuids = []string{}
	r, _ = b.Send(
		gremlin.Query(`g.V(id1).in().id()`).Bindings(gremlin.Bind{"id1": id1}),
	)
	json.Unmarshal(r, &uuids)
	assert.Equal(t, []string{}, uuids, "")

	b.Stop()
}

func TestVertexProperties(t *testing.T) {
	b := NewServerBackend("ws://127.0.0.1:8182/gremlin")
	b.Start()

	id1, _ := uuid.NewV4()
	v1 := Vertex{
		ID:    id1,
		Label: "foo",
	}
	v1.AddProperties(map[string]interface{}{
		"prop1": 1,
		"prop2": false,
		"prop3": []string{"f", "o", "o"},
	})
	v1.AddProperty("prop2", true)
	b.CreateVertex(v1)

	var uuids []string
	r, _ := b.Send(
		gremlin.Query(`g.V(id1).has('prop1', 1).has('prop2', false).has('prop2', true).has('prop3', ["f", "o", "o"]).id()`).Bindings(
			gremlin.Bind{"id1": id1},
		),
	)
	json.Unmarshal(r, &uuids)
	assert.Equal(t, []string{id1.String()}, uuids, "")

	b.Stop()
}

func TestEdgeProperty(t *testing.T) {
	b := NewServerBackend("ws://127.0.0.1:8182/gremlin")
	b.Start()

	id1, _ := uuid.NewV4()
	v1 := Vertex{
		ID:    id1,
		Label: "foo",
	}
	b.CreateVertex(v1)

	id2, _ := uuid.NewV4()
	v2 := Vertex{
		ID:    id2,
		Label: "bar",
	}
	e2 := Edge{
		Label: "ref",
		InV:   v1.ID,
		OutV:  v2.ID,
	}
	e2.AddProperty("prop1", "foo")
	e2.AddProperty("prop2", false)
	e2.AddProperty("prop2", true)
	e2.AddProperty("prop3", 1)
	v2.AddInEdge(e2)
	b.CreateVertex(v2)

	var uuids []string
	r, _ := b.Send(
		gremlin.Query(`g.V(id2).outE().has('prop1', 'foo').has('prop2', true).has('prop3', 1).inV().id()`).Bindings(
			gremlin.Bind{"id2": id2},
		),
	)
	json.Unmarshal(r, &uuids)
	assert.Equal(t, []string{id1.String()}, uuids, "")

	b.Stop()
}

func TestEdgeDiff(t *testing.T) {
	b := NewServerBackend("ws://127.0.0.1:8182/gremlin")
	b.Start()

	id1, _ := uuid.NewV4()
	v1 := Vertex{
		ID:    id1,
		Label: "foo",
	}

	b.CreateVertex(v1)

	id2, _ := uuid.NewV4()
	v2 := Vertex{
		ID:    id2,
		Label: "bar",
	}
	b.CreateVertex(v2)

	e2 := Edge{
		InV:   id2,
		OutV:  id1,
		Label: "ref",
	}
	v2.AddOutEdge(e2)

	toAdd, toUpdate, toRemove, _ := b.diffVertexEdges(v2)
	assert.Equal(t, 1, len(toAdd))
	assert.Equal(t, 0, len(toUpdate))
	assert.Equal(t, 0, len(toRemove))

	b.UpdateVertex(v2)

	toAdd, toUpdate, toRemove, _ = b.diffVertexEdges(v2)
	assert.Equal(t, 0, len(toAdd))
	assert.Equal(t, 0, len(toUpdate))
	assert.Equal(t, 0, len(toRemove))

	v2.OutE = map[string][]Edge{}
	e2.Label = "parent"
	v2.AddOutEdge(e2)

	toAdd, toUpdate, toRemove, _ = b.diffVertexEdges(v2)
	assert.Equal(t, 1, len(toAdd))
	assert.Equal(t, 0, len(toUpdate))
	assert.Equal(t, 1, len(toRemove))

	v2.OutE = map[string][]Edge{}
	e2.Properties = map[string]Property{}
	e2.Label = "ref"
	e2.AddProperty("foo", "bar")
	v2.AddOutEdge(e2)

	toAdd, toUpdate, toRemove, _ = b.diffVertexEdges(v2)
	assert.Equal(t, 0, len(toAdd))
	assert.Equal(t, 1, len(toUpdate))
	assert.Equal(t, 0, len(toRemove))

	v3 := Vertex{
		ID:    id2,
		Label: "bar",
	}
	e3 := Edge{
		InV:   id2,
		OutV:  id1,
		Label: "ref",
	}
	v3.AddOutEdge(e3)

	toAdd, toUpdate, toRemove, _ = b.diffVertexEdges(v3)
	assert.Equal(t, 0, len(toAdd))
	assert.Equal(t, 0, len(toUpdate))
	assert.Equal(t, 0, len(toRemove))

	b.Stop()
}
