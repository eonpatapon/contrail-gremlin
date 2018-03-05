package gremlin

import (
	"github.com/satori/go.uuid"
)

type Property struct {
	Value interface{} `json:"value"`
}

type Edge struct {
	OutV       uuid.UUID           `json:"outV"`
	OutVLabel  string              `json:"outVLabel"`
	InV        uuid.UUID           `json:"inV"`
	InVLabel   string              `json:"inVLabel"`
	Label      string              `json:"label"`
	Properties map[string]Property `json:"properties,omitempty"`
}

func (e *Edge) AddProperties(props map[string]interface{}) {
	for name, value := range props {
		e.AddProperty(name, value)
	}
}

func (e *Edge) AddProperty(name string, value interface{}) {
	if e.Properties == nil {
		e.Properties = make(map[string]Property, 0)
	}
	e.Properties[name] = Property{Value: value}
}

type Vertex struct {
	ID         uuid.UUID             `json:"id"`
	Label      string                `json:"label"`
	Properties map[string][]Property `json:"properties,omitempty"`
	InE        map[string][]Edge     `json:"inE,omitempty"`
	OutE       map[string][]Edge     `json:"outE,omitempty"`
}

func (v *Vertex) AddProperties(props map[string]interface{}) {
	for name, value := range props {
		v.AddProperty(name, value)
	}
}

func (v *Vertex) AddProperty(name string, value interface{}) {
	if v.Properties == nil {
		v.Properties = make(map[string][]Property, 0)
	}
	if _, ok := v.Properties[name]; ok {
		v.Properties[name] = append(v.Properties[name], Property{Value: value})
	} else {
		v.Properties[name] = []Property{Property{Value: value}}
	}
}

func (v *Vertex) AddSingleProperty(name string, value interface{}) {
	if v.Properties == nil {
		v.Properties = make(map[string][]Property, 0)
	}
	v.Properties[name] = []Property{Property{Value: value}}
}

func (v *Vertex) HasProp(name string) bool {
	if _, ok := v.Properties[name]; ok {
		return true
	}
	return false
}

func (v *Vertex) AddInEdge(edge Edge) {
	if v.InE == nil {
		v.InE = make(map[string][]Edge, 0)
	}
	if _, ok := v.InE[edge.Label]; ok {
		v.InE[edge.Label] = append(v.InE[edge.Label], edge)
	} else {
		v.InE[edge.Label] = []Edge{edge}
	}
}

func (v *Vertex) AddOutEdge(edge Edge) {
	if v.OutE == nil {
		v.OutE = make(map[string][]Edge, 0)
	}
	if _, ok := v.OutE[edge.Label]; ok {
		v.OutE[edge.Label] = append(v.OutE[edge.Label], edge)
	} else {
		v.OutE[edge.Label] = []Edge{edge}
	}
}
