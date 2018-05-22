package main

import "strings"

func fieldsToProject(fields []string) string {
	names := make([]string, len(fields))
	for i, name := range fields {
		names[i] = `'` + name + `'`
	}
	return strings.Join(names, ",")
}
