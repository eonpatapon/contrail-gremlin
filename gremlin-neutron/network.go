package main

import (
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
		query    = &gremlinQuery{}
		bindings = gremlin.Bind{}
	)

	if r.Context.IsAdmin {
		query.Add(`g.V().hasLabel('virtual_network').hasNot('_missing')`)
	} else {
		query.Add(`g.V(_tenant_id).in('parent').hasLabel('virtual_network').hasNot('_missing')`)
		query.Add(`.where(values('id_perms').select('user_visible').is(true))`)
		bindings["_tenant_id"] = r.Context.TenantID
	}

	// Add filters to the query
	filterQuery(query, bindings, r.Data.Filters,
		func(query *gremlinQuery, key string, valuesQuery string) {
			switch key {
			case "tenant_id":
				// Add this filter only in admin context, because in user context
				// the collection is already filtered above.
				if r.Context.IsAdmin {
					query.Addf(`.where(__.out('parent').has(id, %s))`, valuesQuery)
				}
			case "router:external":
				query.Addf(`.has('router_external', %s)`, valuesQuery)
			case "shared":
				query.Addf(`.has('is_shared', %s)`, valuesQuery)
			default:
				log.Warningf("No implementation for filter %s", key)
			}
		})

	// Check that requested fields have an implementation
	fields := validateFields(r.Data.Fields, networkDefaultFields)

	query.Addf(".project(%s)", fieldsToProject(fields))

	for _, field := range fields {
		switch field {
		case "id":
			query.Add(`.by(id)`)
		case "tenant_id":
			query.Add(`.by(__.out('parent').id().map{ it.get().toString().replace('-', '') })`)
		case "name":
			query.Add(`.by(
				coalesce(
					values('display_name'),
					constant('')
				)
			)`)
		case "description":
			query.Add(`.by(
				coalesce(
					values('id_perms').select('description'),
					constant('')
				)
			)`)
		case "created_at":
			query.Add(`.by(values('id_perms').select('created'))`)
		case "updated_at":
			query.Add(`.by(values('id_perms').select('last_modified'))`)
		case "admin_state_up":
			query.Add(`.by(values('id_perms').select('enable'))`)
		case "router:external":
			query.Add(`.by(
				coalesce(
					values('router_external'),
					constant(false)
				)
			)`)
		case "shared":
			query.Add(`.by(
				coalesce(
					values('is_shared'),
					constant(false)
				)
			)`)
		case "port_security_enabled":
			query.Add(`.by(
				coalesce(
					values('port_security_enabled'),
					constant(false)
				)
			)`)
		case "subnets":
			query.Add(`.by(
				coalesce(
					__.outE('ref').where(__.otherV().hasLabel('network_ipam'))
					  .values('ipam_subnets').unfold().select('subnet_uuid').fold(),
					constant([])
				)
			)`)
		case "status":
			query.Add(`.by(
				choose(
					values('id_perms').select('enable'),
					constant('ACTIVE'),
					constant('DOWN'),
				)
			)`)
		}
	}

	return a.sendGremlinQuery(query, bindings)
}
