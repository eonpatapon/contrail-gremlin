package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Jeffail/gabs"
	"github.com/gocql/gocql"
	"github.com/satori/go.uuid"
	"github.com/willfaught/gockle"

	g "github.com/eonpatapon/contrail-gremlin/gremlin"
)

var (
	noFlatten = map[string]bool{
		"attr.ipam_subnets":                    true,
		"access_control_list_entries.acl_rule": true,
		"security_group_entries.policy_rule":   true,
		"vrf_assign_table.vrf_assign_rule":     true,
	}
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

func GetContrailResource(session gockle.Session, rUUID uuid.UUID) (g.Vertex, error) {
	var (
		column1   string
		valueJSON []byte
	)
	rows, err := session.ScanMapSlice(`SELECT key, column1, value FROM obj_uuid_table WHERE key=?`, rUUID.String())
	if err != nil {
		return g.Vertex{}, err
	}
	vertex := g.Vertex{
		ID: rUUID,
	}
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
			if len(valueJSON) > 0 {
				value, err := parseJSON(valueJSON)
				if err != nil {
					fmt.Println(fmt.Errorf("Failed to parse %v", string(valueJSON)))
				} else {
					props := make(map[string]interface{})
					flattenJSON("", value, props)
					edge.AddProperties(props)
				}
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
			if len(valueJSON) > 0 {
				value, err := parseJSON(valueJSON)
				if err != nil {
					fmt.Println(fmt.Errorf("Failed to parse %v", string(valueJSON)))
				} else {
					props := make(map[string]interface{})
					flattenJSON("", value, props)
					edge.AddProperties(props)
				}
			}
			vertex.AddInEdge(edge)
		case "type":
			var value string
			json.Unmarshal(valueJSON, &value)
			vertex.Label = value
		case "fq_name":
			var value []string
			json.Unmarshal(valueJSON, &value)
			vertex.AddProperty("fq_name", value)
			// TODO: props
		case "prop":
			value, err := parseJSON(valueJSON)
			if err != nil {
				fmt.Println(fmt.Errorf("Failed to parse %v", string(valueJSON)))
			} else {
				props := make(map[string]interface{})
				flattenJSON(split[1], value, props)
				vertex.AddProperties(props)
			}
		}
	}

	if len(vertex.Label) == 0 {
		vertex.Label = "_incomplete"
		vertex.AddProperty("_incomplete", true)
	}
	if _, ok := vertex.Properties["fq_name"]; !ok {
		vertex.AddProperty("_incomplete", true)
	}
	//if _, ok := vertex.Properties["id_perms.created"]; !ok {
	//vertex.AddProperty("_incomplete", true)
	//}

	// Add updated/created/deleted properties timestamps
	if created, ok := vertex.Properties["id_perms.created"]; ok {
		for _, prop := range created {
			if time, err := time.Parse(time.RFC3339Nano, prop.Value.(string)+`Z`); err == nil {
				vertex.AddProperty("created", time.Unix())
			}
		}
	}
	if updated, ok := vertex.Properties["id_perms.last_modified"]; ok {
		for _, prop := range updated {
			if time, err := time.Parse(time.RFC3339Nano, prop.Value.(string)+`Z`); err == nil {
				vertex.AddProperty("updated", time.Unix())
			}
		}
	}
	vertex.AddProperty("deleted", 0)

	return vertex, nil
}

func parseJSON(valueJSON []byte) (*gabs.Container, error) {
	dec := json.NewDecoder(bytes.NewReader(valueJSON))
	dec.UseNumber()
	return gabs.ParseJSONDecoder(dec)
}

func flattenJSON(prefix string, c *gabs.Container, result map[string]interface{}) {
	if _, ok := c.Data().([]interface{}); ok {
		childs, _ := c.Children()
		for i, child := range childs {
			flattenJSON(fmt.Sprintf("%s.%d", prefix, i), child, result)
		}
		return
	}
	if _, ok := c.Data().(map[string]interface{}); ok {
		childs, _ := c.ChildrenMap()
		for key, child := range childs {
			var pfix string
			if prefix != "" {
				pfix = fmt.Sprintf("%s.%s", prefix, key)
			} else {
				pfix = key
			}
			if _, ok := noFlatten[pfix]; ok {
				result[pfix] = child.String()
			} else {
				flattenJSON(pfix, child, result)
			}
		}
		return
	}
	if prefix == "" {
		return
	}
	if str, ok := c.Data().(string); ok {
		result[prefix] = str
		return
	}
	if boul, ok := c.Data().(bool); ok {
		result[prefix] = boul
	}
	if num, ok := c.Data().(json.Number); ok {
		if n, err := num.Int64(); err == nil {
			result[prefix] = n
			return
		}
		if n, err := num.Float64(); err == nil {
			result[prefix] = n
			return
		}
	}
}
