package main

import (
	"fmt"
	"strings"

	"github.com/eonpatapon/gremlin"
)

type gremlinQuery struct {
	strings.Builder
}

func (q *gremlinQuery) Add(step string) {
	q.WriteString(strings.Join(strings.Fields(step), ""))
}

func (q *gremlinQuery) Addf(step string, args ...interface{}) {
	q.Add(fmt.Sprintf(step, args...))
}

func implemNames() []string {
	implemsNames := make([]string, len(allImplems))
	i := 0
	for k, _ := range allImplems {
		implemsNames[i] = k
		i++
	}
	return implemsNames
}

func validateFields(wantedFields, defaultFields []string) (fields []string) {
	if len(wantedFields) > 0 {
		var found bool
		for _, fieldName := range wantedFields {
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
	return fields
}

func fieldsToProject(fields []string) string {
	names := make([]string, len(fields))
	for i, name := range fields {
		names[i] = `'` + name + `'`
	}
	return strings.Join(names, ",")
}

func filterQueryValues(key string, values interface{}, bindings gremlin.Bind) (string, string) {
	var valuesQuery string
	// replace : to avoid gremlin errors (eg: router:external -> router_external)
	key = strings.Replace(key, ":", "_", -1)
	switch values.(type) {
	case []interface{}:
		if len(values.([]interface{})) > 1 {
			bindingNames := make([]string, len(values.([]interface{})))
			for i, value := range values.([]interface{}) {
				// Prefix the binding name with 'f' so that it does not override
				// previous bindings
				bindingNames[i] = fmt.Sprintf("_f%s_%d", key, i)
				bindings[bindingNames[i]] = value
			}
			valuesQuery = fmt.Sprintf(`within(%s)`, strings.Join(bindingNames, `,`))
		} else {
			bindingName := fmt.Sprintf("_f%s", key)
			bindings[bindingName] = values.([]interface{})[0]
			valuesQuery = bindingName
		}
	}
	return key, valuesQuery
}

func filterQuery(query *gremlinQuery, bindings gremlin.Bind, filters map[string][]interface{}, f func(*gremlinQuery, string, string)) {
	// Implementation of filters that are common to all type of resources
	// Per resource implementation if provided in a callback function
	for key, values := range filters {
		key, valuesQuery := filterQueryValues(key, values, bindings)
		switch key {
		case "id":
			query.Addf(`.has(id, %s)`, valuesQuery)
		case "name":
			query.Addf(`.has('display_name', %s)`, valuesQuery)
		case "description":
			query.Addf(`.where(values('id_perms').select('description').is(%s))`, valuesQuery)
		case "admin_state_up":
			query.Addf(`.where(values('id_perms').select('enable').is(%s))`, valuesQuery)
		default:
			f(query, key, valuesQuery)
		}
	}
}

func valuesQuery(query *gremlinQuery, fields []string, defaultFields []string, f func(*gremlinQuery, string)) {
	// Check that requested fields have an implementation
	validatedFields := validateFields(fields, defaultFields)
	query.Addf(".project(%s)", fieldsToProject(validatedFields))
	// Implementation of values that are common to all type of resources
	// Per resource implementation if provided in a callback function
	for _, field := range validatedFields {
		field = strings.Replace(field, ":", "_", -1)
		switch field {
		case "id":
			query.Add(`.by(id)`)
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
		default:
			f(query, field)
		}
	}
}
