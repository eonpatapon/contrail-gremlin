package gremlin

import (
	"fmt"
)

func TransformVertex(v Vertex) (Vertex, error) {
	switch v.Label {
	case "virtual_machine_interface":
		return transformVMI(v)
	default:
		return v, nil
	}
}

func transformVMI(v Vertex) (Vertex, error) {
	path := "virtual_machine_interface_allowed_address_pairs.allowed_address_pair"
	if aaps, ok := v.PropertyValue(path); ok {
		neutronAAPs := make([]interface{}, 0)
		for _, aap := range aaps.([]interface{}) {
			ip := aap.(map[string]interface{})["ip"].(map[string]interface{})["ip_prefix"]
			neutronAAPs = append(neutronAAPs, map[string]interface{}{
				"ip_address":  fmt.Sprintf("%s", ip),
				"mac_address": aap.(map[string]interface{})["mac"],
			})
		}
		v.AddProperty("neutron.allowed_address_pairs", neutronAAPs)
	}
	return v, nil
}
