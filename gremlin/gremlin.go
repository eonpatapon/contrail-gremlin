package gremlin

import (
	"encoding/json"
	"strings"

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
	e.Properties[name] = Property{Value: sanitizePropertyValue(value)}
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
		v.Properties[name] = append(v.Properties[name], Property{Value: sanitizePropertyValue(value)})
	} else {
		v.Properties[name] = []Property{Property{Value: sanitizePropertyValue(value)}}
	}
}

func (v *Vertex) AddSingleProperty(name string, value interface{}) {
	if v.Properties == nil {
		v.Properties = make(map[string][]Property, 0)
	}
	v.Properties[name] = []Property{Property{Value: value}}
}

// PropertyValue can find a value in a map[string]interface{} Property value
func (v *Vertex) PropertyValue(path string) (interface{}, bool) {
	keys := strings.Split(path, ".")
	if data, ok := v.Properties[keys[0]]; ok {
		if len(keys) == 1 {
			return data, true
		}
		return findValue(keys[1:], data[0].Value)
	}
	return nil, false
}

func findValue(path []string, data interface{}) (interface{}, bool) {
	switch data.(type) {
	case map[string]interface{}:
		next, ok := data.(map[string]interface{})[path[0]]
		if !ok {
			return nil, false
		}
		if len(path) == 1 {
			return next, true
		}
		return findValue(path[1:], next)
	default:
		return nil, true
	}
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

func sanitizePropertyValue(value interface{}) interface{} {
	switch value.(type) {
	case map[string]interface{}:
		for k, v := range value.(map[string]interface{}) {
			value.(map[string]interface{})[k] = sanitizePropertyValue(v)
		}
		return value
	case []interface{}:
		for i, v := range value.([]interface{}) {
			value.([]interface{})[i] = sanitizePropertyValue(v)
		}
		return value
	case string:
		return value.(string)
	case bool:
		return value.(bool)
	case json.Number:
		if n, err := value.(json.Number).Int64(); err == nil {
			return n
		}
		if n, err := value.(json.Number).Float64(); err == nil {
			return n
		}
		return value.(json.Number).String()
	default:
		return value
	}
}
