package main

import (
	"fmt"
	"strings"

	"github.com/eonpatapon/gremlin"
	uuid "github.com/satori/go.uuid"
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

func (a *App) sendGremlinQuery(query *gremlinQuery, bindings gremlin.Bind) ([]byte, error) {
	queryString := query.String()
	uuid, _ := uuid.NewV4()
	requestArgs := &gremlin.RequestArgs{
		Gremlin:  queryString,
		Language: "gremlin-groovy",
		Bindings: bindings,
	}
	if graphName != "g" {
		requestArgs.Aliases = map[string]string{
			"g": graphName,
		}
	}
	request := &gremlin.Request{
		RequestId: uuid.String(),
		Op:        "eval",
		Args:      requestArgs,
	}
	log.Debugf("Request: %+v", *requestArgs)
	res, err := a.gremlinClient.Send(request)
	if err != nil {
		return []byte{}, err
	}
	// TODO: check why gremlinClient does not return an empty list
	if len(res) == 0 {
		return []byte("[]"), nil
	}
	return res, nil
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

func filterQueryValues(key string, values interface{}, bindings gremlin.Bind) (string, gremlin.Bind) {
	var valuesQuery string
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
	return valuesQuery, bindings
}

func filterQuery(query *gremlinQuery, bindings gremlin.Bind, filters map[string]interface{}, f func(*gremlinQuery, string, string)) {
	// Implementation of filters that are common to all type of resources
	// Per resource implementation if provided in a callback function
	for key, values := range filters {
		valuesQuery, _ := filterQueryValues(key, values, bindings)
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
