package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/eonpatapon/contrail-gremlin/testutils"
	uuid "github.com/satori/go.uuid"
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
		run("ws://localhost:8182/gremlin", "", "n", implemNames())
	}()
	time.Sleep(1 * time.Second)
}

func makeRequest(resourceType string, op RequestOperation, tenantID string, isAdmin bool, data RequestData) *http.Response {
	tenantUUID, _ := uuid.FromString(tenantID)
	reqID, _ := uuid.NewV4()
	req := Request{
		Context: RequestContext{
			Type:      resourceType,
			Operation: op,
			TenantID:  tenantUUID,
			RequestID: fmt.Sprintf("req-%s", reqID),
			IsAdmin:   isAdmin,
		},
		Data: data,
	}
	reqJSON, _ := json.Marshal(req)
	resp, _ := http.Post("http://localhost:8080/neutron/port", "application/json", bytes.NewReader(reqJSON))
	return resp
}
