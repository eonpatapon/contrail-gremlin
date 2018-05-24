package main

import (
	"fmt"

	"github.com/eonpatapon/gremlin"
)

var networkDefaultFields = []string{
	"id",
	"tenant_id",
	"name",
	"description",
	"router:external",
	"shared",
	"subnets",
	"status",
	"admin_state_up",
	"port_security_enabled",
	"created_at",
	"updated_at",
}

func (a *App) listNetworks(r Request) ([]byte, error) {
	var (
		query    string
		bindings = gremlin.Bind{}
	)

	if r.Context.IsAdmin {
		query = `g.V().hasLabel('virtual_network').hasNot('_missing')`
	} else {
		query = `g.V(_tenant_id).in('parent').hasLabel('virtual_network').hasNot('_missing')` +
			`.where(values('id_perms').select('user_visible').is(true))`
		bindings["_tenant_id"] = r.Context.TenantID
	}

	// Add filters to the query
	for key, values := range r.Data.Filters {
		valuesQuery, bindings := filterQueryValues(key, values, bindings)
		switch key {
		case "id":
			query += fmt.Sprintf(`.has(id, %s)`, valuesQuery)
		case "name":
			query += fmt.Sprintf(`.has('display_name', %s)`, valuesQuery)
		case "tenant_id":
			// Add this filter only in admin context, because in user context
			// the collection is already filtered above.
			if r.Context.IsAdmin {
				query += fmt.Sprintf(`.where(__.out('parent').has(id, %s))`, valuesQuery)
			}
		case "admin_state_up":
			query += fmt.Sprintf(`.where(values('id_perms').select('enable').is(%s))`, valuesQuery)
		case "router:external":
			query += fmt.Sprintf(`.has('router_external', %s)`, valuesQuery)
		case "shared":
			query += fmt.Sprintf(`.has('is_shared', %s)`, valuesQuery)
		}
	}

	// Check that requested fields have an implementation
	fields := validateFields(r.Data.Fields, networkDefaultFields)

	query += fmt.Sprintf(".project(%s)", fieldsToProject(fields))

	for _, field := range fields {
		switch field {
		case "id":
			query += `.by(id)`
		case "tenant_id":
			query += `.by(__.out('parent').id().map{ it.get().toString().replace('-', '') })`
		case "name":
			query += `.by(
				coalesce(
					values('display_name'),
					constant('')
				)
			)`
		case "description":
			query += `.by(
				coalesce(
					values('id_perms').select('description'),
					constant('')
				)
			)`
		case "created_at":
			query += `.by(values('id_perms').select('created'))`
		case "updated_at":
			query += `.by(values('id_perms').select('last_modified'))`
		case "admin_state_up":
			query += `.by(values('id_perms').select('enable'))`
		case "router:external":
			query += `.by(
				coalesce(
					values('router_external'),
					constant(false)
				)
			)`
		case "shared":
			query += `.by(
				coalesce(
					values('is_shared'),
					constant(false)
				)
			)`
		case "port_security_enabled":
			query += `.by(
				coalesce(
					values('port_security_enabled'),
					constant(false)
				)
			)`
		case "subnets":
			query += `.by(
				coalesce(
					__.outE('ref').where(__.otherV().hasLabel('network_ipam'))
					  .values('ipam_subnets').unfold().select('subnet_uuid').fold(),
					constant([])
				)
			)`
		case "status":
			query += `.by(
				choose(
					values('id_perms').select('enable'),
					constant('ACTIVE'),
					constant('DOWN'),
				)
			)`
		}
	}

	return a.sendGremlinQuery(query, bindings)
}
