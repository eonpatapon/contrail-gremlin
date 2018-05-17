package gremlin

import (
	"encoding/json"
	"fmt"

	"github.com/eonpatapon/contrail-gremlin/neutron"
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
		log.Errorf("%+v", v.Properties[aapProp])
		var (
			aaps        []AllowAddressPair
			neutronAaps = []neutron.AAP{}
		)
		data := v.Properties[aapProp][0].Value.(string)
		json.Unmarshal([]byte(data), &aaps)
		log.Errorf("%+v", aaps)
		for _, aap := range aaps {
			neutronAaps = append(neutronAaps, neutron.AAP{
				IP:  fmt.Sprintf("%s/%d", aap.IP.Prefix, aap.IP.Len),
				MAC: aap.Mac,
			})
		}
		res, err := json.Marshal(neutronAaps)
		if err != nil {
			log.Errorf("Failed to generate neutron AAPs")
		} else {
			v.AddProperty("neutron.allowed_address_pairs", string(res))
		}
	}
	return v, nil
}
