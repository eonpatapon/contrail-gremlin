package main

import (
	"strings"

	"github.com/eonpatapon/gremlin"
)

var portDefaultFields = []string{
	"id",
	"tenant_id",
	"network_id",
	"name",
	"description",
	"security_groups",
	"fixed_ips",
	"mac_address",
	"allowed_address_pairs",
	"device_id",
	"device_owner",
	"status",
	"admin_state_up",
	"binding:vif_details",
	"binding:vif_type",
	"binding:vnic_type",
	"binding:host_id",
	"created_at",
	"updated_at",
}

func (a *App) listPorts(r Request) ([]byte, error) {

	if values, ok := r.Data.Filters["device_owner"]; ok {
		for _, value := range values {
			if value == "network:dhcp" {
				return []byte("[]"), nil
			}
		}
	}

	var (
		query    = &gremlinQuery{}
		bindings = gremlin.Bind{}
	)

	if r.Context.IsAdmin {
		query.Add(`g.V().hasLabel('virtual_machine_interface')`)
	} else {
		query.Add(`g.V(_tenant_id).in('parent').hasLabel('virtual_machine_interface')`)
		query.Add(`.where(values('id_perms').select('user_visible').is(true))`)
		bindings["_tenant_id"] = r.Context.TenantID
	}

	// Flatten some complex filters
	for key, values := range r.Data.Filters {
		switch key {
		case "fixed_ips":
			for _, value := range values {
				filter := strings.Split(value.(string), "=")
				if _, ok := r.Data.Filters[filter[0]]; ok {
					r.Data.Filters[filter[0]] = append(r.Data.Filters[filter[0]], filter[1])
				} else {
					r.Data.Filters[filter[0]] = []interface{}{filter[1]}
				}
			}
		}
	}

	filterQuery(query, bindings, r.Data.Filters,
		func(query *gremlinQuery, key string, valuesQuery string) {
			switch key {
			case "tenant_id":
				// Add this filter only in admin context, because in user context
				// the collection is already filtered above.
				if r.Context.IsAdmin {
					query.Addf(`.where(__.out('parent').has(id, %s))`, valuesQuery)
				}
			case "network_id":
				query.Addf(`.where(__.out('ref').hasLabel('virtual_network').has(id, %s))`, valuesQuery)
			case "device_owner":
				query.Addf(`.has('virtual_machine_interface_device_owner', %s)`, valuesQuery)
			case "device_id":
				// Check for VMs and LRs
				query.Addf(`.where(__.both('ref').has(id, %s))`, valuesQuery)
			case "ip_address":
				query.Addf(`where(
					__.in('ref').hasLabel('instance_ip').has('instance_ip_address', %s)
				)`, valuesQuery)
			case "subnet_id":
				query.Addf(`.where(
					__.in('ref').hasLabel('instance_ip').has('subnet_uuid', %s)
				)`, valuesQuery)
			case "fixed_ips":
				// This is handled by "ip_address" and "subnet_id" cases.
			default:
				log.Warningf("No implementation for filter %s", key)
			}
		})

	valuesQuery(query, r.Data.Fields, portDefaultFields,
		func(query *gremlinQuery, field string) {
			switch field {
			case "tenant_id":
				query.Add(`.by(__.out('parent').id().map{ it.get().toString().replace('-', '') })`)
			case "network_id":
				query.Add(`.by(__.out('ref').hasLabel('virtual_network').id())`)
			case "security_groups":
				query.Add(`.by(
					__.out('ref').hasLabel('security_group')
						.not(has('fq_name', ['default-domain', 'default-project', '__no_rule__']))
						.id().fold()
				)`)
			case "fixed_ips":
				query.Add(`.by(
					__.in('ref').hasLabel('instance_ip')
						.project('ip_address', 'subnet_id')
							.by('instance_ip_address')
							.by(coalesce(values('subnet_uuid'), constant('')))
						.fold()
				)`)
			case "mac_address":
				query.Add(`.by(
					coalesce(
						values('virtual_machine_interface_mac_addresses').select('mac_address').unfold(),
						constant('')
					)
				)`)
			case "allowed_address_pairs":
				query.Add(`.by(
					coalesce(
						values('neutron.allowed_address_pairs'),
						constant([])
					)
				)`)
			case "device_id":
				query.Add(`.by(
					coalesce(
						__.out('ref').hasLabel('virtual_machine').id(),
						__.in('ref').hasLabel('logical_router').id(),
						constant('')
					)
				)`)
			case "device_owner":
				query.Add(`.by(
					coalesce(
						values('virtual_machine_interface_device_owner'),
						constant('')
					)
				)`)
			case "status":
				query.Add(`.by(
					choose(
						__.has('virtual_machine_interface_device_owner'),
						constant('ACTIVE'),
						constant('DOWN'),
					)
				)`)
			case "binding:vif_details":
				query.Add(`.by(constant([ port_filter : true ]))`)
			case "binding:vif_type":
				query.Add(`.by(constant('vrouter'))`)
			case "binding:vnic_type":
				query.Add(`.by(constant('normal'))`)
			case "binding:host_id":
				query.Add(`.by(constant('none'))`)
			case "created_at":
				query.Add(`.by(values('id_perms').select('created'))`)
			case "updated_at":
				query.Add(`.by(values('id_perms').select('last_modified'))`)
			}
		})

	return a.sendGremlinQuery(query, bindings)
}
