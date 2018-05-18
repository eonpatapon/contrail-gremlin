package gremlin

import (
	"encoding/json"
	"fmt"
)

type IP struct {
	Prefix string `json:"ip_prefix"`
	Len    int    `json:"ip_prefix_len"`
}

type AllowAddressPair struct {
	AddressMode string `json:"address_mode"`
	IP          IP     `json:"ip"`
	Mac         string `json:"mac"`
}

func TransformVertex(v Vertex) (Vertex, error) {
	switch v.Label {
	case "virtual_machine_interface":
		return transformVMI(v)
	default:
		return v, nil
	}
}

func transformVMI(v Vertex) (Vertex, error) {
	aapProp := "virtual_machine_interface_allowed_address_pairs.allowed_address_pair"
	if v.HasProp(aapProp) && len(v.Properties[aapProp]) > 0 {
		var (
			aaps        []AllowAddressPair
			neutronAAPs = make([]interface{}, 0)
		)
		data := v.Properties[aapProp][0].Value.(string)
		err := json.Unmarshal([]byte(data), &aaps)
		if err != nil {
			fmt.Printf("%s\n", err)
			return v, nil
		}
		for _, aap := range aaps {
			neutronAAPs = append(neutronAAPs, map[string]interface{}{
				"ip_address":  fmt.Sprintf("%s/%d", aap.IP.Prefix, aap.IP.Len),
				"mac_address": aap.Mac,
			})
		}
		v.AddProperty("neutron.allowed_address_pairs", neutronAAPs)
	}
	return v, nil
}
