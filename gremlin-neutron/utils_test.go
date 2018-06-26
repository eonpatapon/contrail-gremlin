package main

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/eonpatapon/contrail-gremlin/testutils"
	"github.com/stretchr/testify/assert"
)

var tenantID = "0ed483e083ef4f7082501fcfa5d98c0e"

func TestMain(m *testing.M) {
	cmd := testutils.StartGremlinServerWithDump("gremlin-neutron.yml", "2305.json")
	start()
	res := m.Run()
	stop()
	testutils.StopGremlinServer(cmd)
	os.Exit(res)
}

func TestFiltersUnmarshal(t *testing.T) {
	data := `
	{
		"context": {},
		"data": {
			"filters": {
				"foo": {
					"filter1": ["a", "b"]
				},
				"bar": [true]
			}
		}
	}`
	req := Request{
		Data: RequestData{
			Filters: make(RequestFilters, 0),
		},
	}
	if err := json.Unmarshal([]byte(data), &req); err != nil {
		panic(fmt.Sprintf("%s: %s", string(data), err))
	}

	expected := Request{
		Data: RequestData{
			Filters: RequestFilters{
				"filter1": []interface{}{"a", "b"},
				"bar":     []interface{}{true},
			},
		},
	}

	assert.Equal(t, expected, req)

}

func start() {
	go func() {
		run("ws://localhost:8182/gremlin", "", "n")
	}()
	time.Sleep(1 * time.Second)
}
