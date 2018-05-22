package main

import (
	"fmt"
	"strings"

	"github.com/eonpatapon/gremlin"
)

var defaultFields = []string{
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
		query    string
		bindings = gremlin.Bind{}
	)

	if r.Context.IsAdmin {
		query = `g.V().hasLabel('virtual_machine_interface')`
	} else {
		query = `g.V(_tenant_id).in('parent').hasLabel('virtual_machine_interface')` +
			`.where(values('id_perms').select('user_visible').is(true))`
		bindings["_tenant_id"] = r.Context.TenantID
	}

	for key, values := range r.Data.Filters {
		var valuesQuery string
		if len(values) > 1 {
			bindingNames := make([]string, len(values))
			for i, value := range values {
				bindingNames[i] = fmt.Sprintf("_%s_%d", key, i)
				bindings[bindingNames[i]] = value
			}
			valuesQuery = fmt.Sprintf(`within(%s)`, strings.Join(bindingNames, `,`))
		} else {
			bindingName := fmt.Sprintf("_%s", key)
			bindings[bindingName] = values[0]
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

	var fields []string
	if len(r.Data.Fields) > 0 {
		var found bool
		for _, fieldName := range r.Data.Fields {
			found = false
			for _, defaultFieldName := range defaultFields {
				if fieldName == defaultFieldName {
					found = true
					break
				}
			}
			if found {
				fields = append(fields, fieldName)
			} else {
				log.Warningf("No implementation for field %s", fieldName)
			}
		}
	} else {
		fields = defaultFields
	}

	query += fmt.Sprintf(".project(%s)", fieldsToProject(fields))

	for _, field := range fields {
		switch field {
		case "id":
			query += `.by(id)`
		case "tenant_id":
			query += `.by(__.out('parent').id().map{ it.get().toString().replace('-', '') })`
		case "network_id":
			query += `.by(__.out('ref').hasLabel('virtual_network').id())`
		case "name":
			query += `.by('display_name')`
		case "description":
			query += `.by(
				coalesce(
					values('id_perms.description'),
					constant('')
				)
			)`
		case "security_groups":
			query += `.by(
				__.out('ref').hasLabel('security_group')
					.not(has('fq_name', ['default-domain', 'default-project', '__no_rule__']))
					.id().fold()
			)`
		case "fixed_ips":
			query += `.by(
				__.in('ref').hasLabel('instance_ip')
					.project('ip_address', 'subnet_id')
						.by('instance_ip_address')
						.by(coalesce(values('subnet_uuid'), constant('')))
					.fold()
			)`
		case "mac_address":
			query += `.by(
				coalesce(
					values('virtual_machine_interface_mac_addresses').select('mac_address').unfold(),
					constant('')
				)
			)`
		case "allowed_address_pairs":
			query += `.by(
				coalesce(
					values('neutron.allowed_address_pairs'),
					constant([])
				)
			)`
		case "device_id":
			query += `.by(
				coalesce(
					__.out('ref').hasLabel('virtual_machine').id(),
					__.in('ref').hasLabel('logical_router').id(),
					constant('')
				)
			)`
		case "device_owner":
			query += `.by(
				coalesce(
					values('virtual_machine_interface_device_owner'),
					constant('')
				)
			)`
		case "status":
			query += `.by(
				choose(
					__.has('virtual_machine_interface_device_owner'),
					constant('ACTIVE'),
					constant('DOWN'),
				)
			)`
		case "admin_state_up":
			query += `.by(values('id_perms').select('enable'))`
		case "binding:vif_details":
			query += `.by(constant([ port_filter : true ]))`
		case "binding:vif_type":
			query += `.by(constant('vrouter'))`
		case "binding:vnic_type":
			query += `.by(constant('normal'))`
		case "binding:host_id":
			query += `.by(constant('none'))`
		case "created_at":
			query += `.by(values('id_perms').select('created'))`
		case "updated_at":
			query += `.by(values('id_perms').select('last_modified'))`
		}
	}

	log.Debugf("Query: %s, Bindings: %+v", query, bindings)

	res, err := a.gremlinClient.Send(gremlin.Query(query).Bindings(bindings))
	if err != nil {
		return []byte{}, err
	}

	log.Debugf("Response: %s", string(res))

	return res, nil
}
