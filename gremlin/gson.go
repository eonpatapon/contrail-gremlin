package gremlin

import (
	"encoding/json"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"github.com/satori/go.uuid"
)

var (
	// ErrDuplicateVertex indicates a vertex with the same
	// ID has been writen to the gson file
	ErrDuplicateVertex = errors.New("Duplicate Vertex")
)

// GsonValue is a GSON value
type GsonValue struct {
	Type  string      `json:"@type"`
	Value interface{} `json:"@value"`
}

func (v *GsonValue) UnmarshalJSON(data []byte) error {
	var value map[string]interface{}
	err := json.Unmarshal(data, &value)
	if err != nil {
		return err
	}
	return v.fill(value)
}

func (v *GsonValue) fill(value map[string]interface{}) error {
	v.Type = value["@type"].(string)
	switch v.Type {
	case "g:UUID":
		uuid, err := uuid.FromString(value["@value"].(string))
		if err != nil {
			return err
		}
		v.Value = uuid
	case "g:Int64":
		v.Value = int64(value["@value"].(float64))
	case "g:Float64":
		v.Value = value["@value"].(float64)
	default:
		v.Value = value["@value"]
	}
	return nil
}

type GsonProperty struct {
	ID    int64       `json:"id"`
	Value interface{} `json:"value"`
}

func (p *GsonProperty) UnmarshalJSON(data []byte) (err error) {
	var prop map[string]interface{}
	err = json.Unmarshal(data, &prop)
	if err != nil {
		return
	}
	p.ID = int64(prop["id"].(float64))
	switch prop["value"].(type) {
	case map[string]interface{}:
		value := GsonValue{}
		err = value.fill(prop["value"].(map[string]interface{}))
		if err != nil {
			return
		}
		p.Value = value
	default:
		p.Value = prop["value"]
	}
	return nil
}

type GsonEdge struct {
	Ref        string                  `json:"-"`
	ID         GsonValue               `json:"id"`
	Properties map[string]GsonProperty `json:"properties,omitempty"`
	OutV       *GsonValue              `json:"outV,omitempty"`
	InV        *GsonValue              `json:"inV,omitempty"`
}

type GsonVertex struct {
	ID         GsonValue                 `json:"id"`
	Label      string                    `json:"label"`
	Properties map[string][]GsonProperty `json:"properties,omitempty"`
	InE        map[string][]GsonEdge     `json:"inE,omitempty"`
	OutE       map[string][]GsonEdge     `json:"outE,omitempty"`
}

func (v GsonVertex) toJSON(indent ...bool) ([]byte, error) {
	if len(indent) > 0 {
		return json.MarshalIndent(v, "", "  ")
	}
	return json.Marshal(v)
}

func (v *GsonVertex) fromJSON(data []byte) error {
	return json.Unmarshal(data, v)
}

// UUID returns the UUID of the current vertex
func (v GsonVertex) UUID() uuid.UUID {
	return v.ID.Value.(uuid.UUID)
}

type GsonBackend struct {
	output  io.Writer
	write   chan Vertex
	written map[uuid.UUID]bool
	pending map[uuid.UUID]Vertex
	propID  *int64           // property ID counter
	edgeID  *int64           // edge ID counter
	edgeIDs map[string]int64 // track edge IDs
	wg      *sync.WaitGroup
	sync.RWMutex
}

func NewGsonBackend(output io.Writer) *GsonBackend {
	return &GsonBackend{
		output:  output,
		write:   make(chan Vertex),
		written: make(map[uuid.UUID]bool),
		pending: make(map[uuid.UUID]Vertex),
		propID:  new(int64),
		edgeID:  new(int64),
		edgeIDs: make(map[string]int64),
		wg:      &sync.WaitGroup{},
	}
}

func (b *GsonBackend) Start() {
	go b.writer()
}

func (b *GsonBackend) Stop() {
	close(b.write)
	b.wg.Wait()
}

func (b *GsonBackend) addPendingV(v Vertex) {
	// First we check that for each edge of the vertex
	// we already have written the other vertex
	// if not we add the other vertex to a pending map

	// ref, parent
	for label, edges := range v.OutE {
		for _, e := range edges {
			if _, ok := b.written[e.InV]; ok {
				continue
			}
			pendingV, ok := b.pending[e.InV]
			if !ok {
				pendingV = Vertex{
					ID:         e.InV,
					Label:      e.InVLabel,
					Properties: map[string][]Property{},
					InE:        map[string][]Edge{},
					OutE:       map[string][]Edge{},
				}
				pendingV.AddSingleProperty("fq_name", []string{"_missing"})
				pendingV.AddSingleProperty("_missing", true)
				b.pending[pendingV.ID] = pendingV
			}
			pendingV.AddInEdge(Edge{
				Label:      label,
				OutV:       v.ID,
				OutVLabel:  v.Label,
				Properties: e.Properties,
			})
		}
	}
	// back_ref, children
	for label, edges := range v.InE {
		for _, e := range edges {
			if _, ok := b.written[e.OutV]; ok {
				continue
			}
			pendingV, ok := b.pending[e.OutV]
			if !ok {
				pendingV = Vertex{
					ID:         e.OutV,
					Label:      e.OutVLabel,
					Properties: map[string][]Property{},
					InE:        map[string][]Edge{},
					OutE:       map[string][]Edge{},
				}
				pendingV.AddProperty("fq_name", []string{"_missing"})
				pendingV.AddProperty("_missing", true)
				b.pending[pendingV.ID] = pendingV
			}
			pendingV.AddOutEdge(Edge{
				Label:      label,
				InV:        v.ID,
				InVLabel:   v.Label,
				Properties: e.Properties,
			})
		}
	}
}

func (b *GsonBackend) writer() {
	b.wg.Add(1)
	defer b.wg.Done()
	for v := range b.write {
		b.addPendingV(v)
		b.writeVertex(v)
	}
	for _, v := range b.pending {
		b.writeVertex(v)
	}
}

func (b *GsonBackend) writeVertex(v Vertex) error {
	gv := b.newGsonVertex(v)
	vJSON, err := gv.toJSON()
	if err != nil {
		return err
	}
	_, err = b.output.Write(vJSON)
	if err != nil {
		return err
	}
	b.Lock()
	b.written[gv.UUID()] = true
	b.Unlock()
	if _, ok := b.pending[v.ID]; ok {
		delete(b.pending, v.ID)
	}
	b.output.Write([]byte("\n"))
	return nil
}

func (b *GsonBackend) newProp(value interface{}) GsonProperty {
	return GsonProperty{
		ID:    atomic.AddInt64(b.propID, 1),
		Value: value,
	}
}

func (b *GsonBackend) newUUIDValue(uuid uuid.UUID) GsonValue {
	return GsonValue{Type: "g:UUID", Value: uuid}
}

func (b *GsonBackend) newInt32Value(value int32) GsonValue {
	return GsonValue{Type: "g:Int32", Value: value}
}

func (b *GsonBackend) newInt64Value(value int64) GsonValue {
	return GsonValue{Type: "g:Int64", Value: value}
}

func (b *GsonBackend) newFloat64Value(value float64) GsonValue {
	return GsonValue{Type: "g:Float64", Value: value}
}

func (b *GsonBackend) getGsonEdgeID(ref string) int64 {
	b.Lock()
	defer b.Unlock()
	if _, ok := b.edgeIDs[ref]; !ok {
		b.edgeIDs[ref] = atomic.AddInt64(b.edgeID, 1)
	}
	return b.edgeIDs[ref]
}

func (b *GsonBackend) newGsonEdge(v Vertex, e Edge, ref string) GsonEdge {
	ge := GsonEdge{
		ID:         b.newInt64Value(b.getGsonEdgeID(ref)),
		Properties: make(map[string]GsonProperty),
	}
	if e.OutV != uuid.Nil && e.OutV != v.ID {
		outV := b.newUUIDValue(e.OutV)
		ge.OutV = &outV
	}
	if e.InV != uuid.Nil && e.InV != v.ID {
		inV := b.newUUIDValue(e.InV)
		ge.InV = &inV
	}
	for name, prop := range e.Properties {
		switch prop.Value.(type) {
		case int:
			ge.Properties[name] = b.newProp(
				b.newInt64Value(int64(prop.Value.(int))))
		case int32:
			ge.Properties[name] = b.newProp(
				b.newInt64Value(int64(prop.Value.(int32))))
		case int64:
			ge.Properties[name] = b.newProp(
				b.newInt64Value(prop.Value.(int64)))
		case float64:
			ge.Properties[name] = b.newProp(
				b.newFloat64Value(prop.Value.(float64)))
		default:
			ge.Properties[name] = b.newProp(prop.Value)
		}

	}
	return ge
}

func (b *GsonBackend) newGsonVertex(v Vertex) GsonVertex {
	gv := GsonVertex{
		ID:    b.newUUIDValue(v.ID),
		Label: v.Label,
	}
	for name, propList := range v.Properties {
		if gv.Properties == nil {
			gv.Properties = make(map[string][]GsonProperty)
		}
		gv.Properties[name] = make([]GsonProperty, 0)
		for _, prop := range propList {
			switch prop.Value.(type) {
			case int:
				gv.Properties[name] = append(gv.Properties[name],
					b.newProp(b.newInt64Value(int64(prop.Value.(int)))))
			case int32:
				gv.Properties[name] = append(gv.Properties[name],
					b.newProp(b.newInt64Value(int64(prop.Value.(int32)))))
			case int64:
				gv.Properties[name] = append(gv.Properties[name],
					b.newProp(b.newInt64Value(prop.Value.(int64))))
			case float64:
				gv.Properties[name] = append(gv.Properties[name],
					b.newProp(b.newFloat64Value(prop.Value.(float64))))
			default:
				gv.Properties[name] = append(gv.Properties[name],
					b.newProp(prop.Value))
			}
		}
	}
	for name, edgeList := range v.InE {
		if gv.InE == nil {
			gv.InE = make(map[string][]GsonEdge)
		}
		gv.InE[name] = make([]GsonEdge, 0)
		for _, edge := range edgeList {
			ref := edge.OutV.String() + "-" + v.ID.String()
			gv.InE[name] = append(gv.InE[name], b.newGsonEdge(v, edge, ref))
		}
	}
	for name, edgeList := range v.OutE {
		if gv.OutE == nil {
			gv.OutE = make(map[string][]GsonEdge)
		}
		gv.OutE[name] = make([]GsonEdge, 0)
		for _, edge := range edgeList {
			ref := v.ID.String() + "-" + edge.InV.String()
			gv.OutE[name] = append(gv.OutE[name], b.newGsonEdge(v, edge, ref))
		}
	}
	return gv
}

func (b *GsonBackend) Create(v Vertex) error {
	b.RLock()
	if _, ok := b.written[v.ID]; ok {
		b.RUnlock()
		return ErrDuplicateVertex
	}
	b.RUnlock()
	b.write <- v
	return nil
}
