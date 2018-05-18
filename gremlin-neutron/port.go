package main

import (
	"fmt"
	"strings"

	"github.com/eonpatapon/gremlin"
)

func (a *App) listPorts(r Request) ([]byte, error) {

	if value, ok := r.Data.Filters["device_owner"]; ok {
		if value == "network:dhcp" {
			return []byte("[]"), nil
		}
	}

	var (
		query    string
		bindings = gremlin.Bind{}
	)

	if r.Context.IsAdmin {
		query = `g.V().hasLabel('virtual_machine_interface')`
	} else {
		query = `g.V(_tenant_id).in('parent').hasLabel('virtual_machine_interface')
				  .has('id_perms.user_visible', true)`
		bindings["_tenant_id"] = r.Context.TenantID
	}

	for key, value := range r.Data.Filters {
		var valuesQuery string
		values := strings.Split(value, ",")
		if len(values) > 1 {
			bindingNames := make([]string, len(values))
			for i, value := range values {
				bindingNames[i] = fmt.Sprintf("_%s_%d", key, i)
				bindings[bindingNames[i]] = value
			}
			valuesQuery = fmt.Sprintf(`within(%s)`, strings.Join(bindingNames, `,`))
		} else {
			bindingName := fmt.Sprintf("_%s", key)
			bindings[bindingName] = value
			valuesQuery = bindingName
		}
		switch key {
		case "id":
			query += fmt.Sprintf(`.has(id, %s)`, valuesQuery)
		case "name":
			query += fmt.Sprintf(`.has('display_name', %s)`, valuesQuery)
		case "network_id":
			query += fmt.Sprintf(`.where(__.out('ref').hasLabel('virtual_network').has(id, %s))`, valuesQuery)
		case "device_owner":
			query += fmt.Sprintf(`.has('virtual_machine_interface_device_owner', %s)`, valuesQuery)
		case "device_id":
			// check for VMs and LRs
			query += fmt.Sprintf(`.where(__.both('ref').has(id, %s))`, valuesQuery)
		}
	}

	query += `
		.project('id', 'tenant_id', 'network_id', 'name', 'description',
				 'security_groups', 'fixed_ips', 'mac_address', 'allowed_address_pairs',
				 'device_id', 'device_owner', 'status', 'admin_state_up',
				 'binding:vif_details', 'bindings:vif_type', 'bindings:vnic_type', 'bindings:host_id',
				 'created_at', 'updated_at')
			.by(id)
			.by(
				__.out('parent').id().map{ it.get().toString().replace('-', '') }
			)
			.by(
				__.out('ref').hasLabel('virtual_network').id()
			)
			.by('display_name')
			.by(
				coalesce(
					values('id_perms.description'),
					constant('')
				)
			)
			.by(
				__.out('ref').hasLabel('security_group')
					.not(has('fq_name', ['default-domain', 'default-project', '__no_rule__']))
					.id().fold()
			)
			.by(
				__.in('ref').hasLabel('instance_ip')
					.project('ip_address', 'subnet_id')
						.by('instance_ip_address')
						.by(coalesce(values('subnet_uuid'), constant('')))
					.fold()
			)
			.by(
				coalesce(
					values('virtual_machine_interface_mac_addresses.mac_address.0'),
					constant('')
				)
			)
			.by(
				coalesce(
					values('neutron.allowed_address_pairs'),
					constant([])
				)
			)
			.by(
				coalesce(
					__.out('ref').hasLabel('virtual_machine').id(),
					__.in('ref').hasLabel('logical_router').id(),
					constant('')
				)
			)
			.by(
				coalesce(
					values('virtual_machine_interface_device_owner'),
					constant('')
				)
			)
			.by(
				choose(
					__.has('virtual_machine_interface_device_owner'),
					constant('ACTIVE'),
					constant('DOWN'),
				)
			)
			.by('id_perms.enable')
			.by(constant([ port_filter : true ]))
			.by(constant('vrouter'))
			.by(constant('normal'))
			.by(constant('none'))
			.by('id_perms.created')
			.by('id_perms.last_modified')
	`

	log.Debugf("%s : %+v", query, bindings)

	res, err := a.gremlinClient.Send(gremlin.Query(query).Bindings(bindings))

	if err != nil {
		return []byte{}, err
	}

	return res, nil
}
