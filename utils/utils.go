package utils

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Jeffail/gabs"
	"github.com/gocql/gocql"
	"github.com/satori/go.uuid"
	"github.com/willfaught/gockle"

	g "github.com/eonpatapon/contrail-gremlin/gremlin"
)

var (
	// ErrResourceNotFound indicates that the resource is not in contrail db
	ErrResourceNotFound = errors.New("resource not found")
)

func SetupCassandra(cassandraCluster []string) (gockle.Session, error) {
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
	return mockableSession, err
}

func GetContrailUUIDs(session gockle.Session, uuids chan uuid.UUID) error {
	var (
		column1 string
	)
	r := session.ScanIterator(`SELECT column1 FROM obj_fq_name_table`)
	for r.Scan(&column1) {
		parts := strings.Split(column1, ":")
		uuid, err := uuid.FromString(parts[len(parts)-1])
		if err == nil {
			uuids <- uuid
		}
	}
	return r.Close()
}

func generateEdgeProperties(valueJSON []byte) (map[string]interface{}, bool) {
	if len(valueJSON) > 0 {
		value, err := parseJSON(valueJSON)
		if err != nil {
			fmt.Println(fmt.Errorf("Failed to parse %v", string(valueJSON)))
		} else {
			switch value.Data().(type) {
			case map[string]interface{}:
				if props, ok := value.Data().(map[string]interface{})["attr"]; ok {
					switch props.(type) {
					case map[string]interface{}:
						return props.(map[string]interface{}), true
					}
				}
			}
		}
	}
	return nil, false
}

func generateVertexProperty(valueJSON []byte) (interface{}, bool) {
	if len(valueJSON) > 0 {
		value, err := parseJSON(valueJSON)
		if err != nil {
			fmt.Println(fmt.Errorf("Failed to parse %v", string(valueJSON)))
		} else {
			return value.Data().(interface{}), true
		}
	}
	return nil, false
}

func GetContrailResource(session gockle.Session, rUUID uuid.UUID) (g.Vertex, error) {
	var (
		column1   string
		valueJSON []byte
	)
	rows, err := session.ScanMapSlice(`SELECT key, column1, value FROM obj_uuid_table WHERE key=?`, rUUID.String())
	if err != nil {
		return g.Vertex{}, err
	}
	if len(rows) == 0 {
		return g.Vertex{}, ErrResourceNotFound
	}
	vertex := g.Vertex{
		ID: rUUID,
	}
	mapProperties := make(map[string]map[string]json.RawMessage, 0)
	listProperties := make(map[string]map[int]json.RawMessage, 0)
	for _, row := range rows {
		column1 = string(row["column1"].([]byte))
		valueJSON = []byte(row["value"].(string))
		split := strings.Split(column1, ":")
		switch split[0] {
		case "parent", "ref":
			label := split[0]
			inVUUID, _ := uuid.FromString(split[2])
			edge := g.Edge{
				Label:    label,
				InV:      inVUUID,
				InVLabel: split[1],
				OutV:     rUUID,
			}
			if props, ok := generateEdgeProperties(valueJSON); ok {
				edge.AddProperties(props)
			}
			vertex.AddOutEdge(edge)
		case "children", "backref":
			var label string
			if split[0] == "backref" {
				label = "ref"
			} else {
				label = "parent"
			}
			outVUUID, _ := uuid.FromString(split[2])
			edge := g.Edge{
				Label:     label,
				OutV:      outVUUID,
				OutVLabel: split[1],
				InV:       rUUID,
			}
			if props, ok := generateEdgeProperties(valueJSON); ok {
				edge.AddProperties(props)
			}
			vertex.AddInEdge(edge)
		case "type":
			var value string
			json.Unmarshal(valueJSON, &value)
			vertex.Label = value
		case "fq_name":
			var value []string
			json.Unmarshal(valueJSON, &value)
			vertex.AddSingleProperty("fq_name", value)
		case "prop":
			if propValue, ok := generateVertexProperty(valueJSON); ok {
				vertex.AddProperty(split[1], propValue)
			}
		case "propm":
			var value map[string]json.RawMessage
			json.Unmarshal(valueJSON, &value)
			if _, ok := mapProperties[split[1]]; !ok {
				mapProperties[split[1]] = make(map[string]json.RawMessage, 0)
			}
			mapProperties[split[1]][split[2]] = value["value"]
		case "propl":
			if _, ok := listProperties[split[1]]; !ok {
				listProperties[split[1]] = make(map[int]json.RawMessage, 0)
			}
			if idx, err := strconv.Atoi(split[2]); err == nil {
				listProperties[split[1]][idx] = valueJSON
			}
		}
	}

	for k, v := range mapProperties {
		if valueJSON, err := json.Marshal(v); err != nil {
			fmt.Println(fmt.Errorf("Failed to marshal %v", v))
		} else {
			if propValue, ok := generateVertexProperty(valueJSON); ok {
				vertex.AddProperty(k, propValue)
			}
		}
	}

	for k, vs := range listProperties {
		propList := make([]json.RawMessage, len(vs))
		for idx, v := range vs {
			propList[idx] = v
		}
		if valueJSON, err := json.Marshal(propList); err != nil {
			fmt.Println(fmt.Errorf("Failed to marshal %v", propList))
		} else {
			if propValue, ok := generateVertexProperty(valueJSON); ok {
				vertex.AddProperty(k, propValue)
			}
		}
	}

	if len(vertex.Label) == 0 {
		vertex.Label = "_incomplete"
		vertex.AddSingleProperty("_incomplete", true)
	}
	if _, ok := vertex.Properties["fq_name"]; !ok {
		vertex.AddSingleProperty("_incomplete", true)
	}
	if _, ok := vertex.Properties["id_perms"]; !ok {
		vertex.AddSingleProperty("_incomplete", true)
	}

	// Add updated/created/deleted properties timestamps
	if created, ok := vertex.PropertyValue("id_perms.created"); ok {
		if time, err := time.Parse(time.RFC3339Nano, created.(string)+`Z`); err == nil {
			vertex.AddSingleProperty("created", time.Unix())
		}
	}
	if updated, ok := vertex.PropertyValue("id_perms.last_modified"); ok {
		if time, err := time.Parse(time.RFC3339Nano, updated.(string)+`Z`); err == nil {
			vertex.AddSingleProperty("updated", time.Unix())
		}
	}

	// Mark the vertex as deleted, but we don't know when it was deleted
	if vertex.HasProp("_incomplete") {
		vertex.AddSingleProperty("deleted", -1)
	} else {
		vertex.AddSingleProperty("deleted", 0)
	}

	return g.TransformVertex(vertex)
}

func parseJSON(valueJSON []byte) (*gabs.Container, error) {
	dec := json.NewDecoder(bytes.NewReader(valueJSON))
	dec.UseNumber()
	return gabs.ParseJSONDecoder(dec)
}
