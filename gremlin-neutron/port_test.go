package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/eonpatapon/contrail-gremlin/neutron"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

var tenantID = "0ed483e083ef4f7082501fcfa5d98c0e"

func start() {
	go func() {
		run("ws://localhost:8182/gremlin", "", "", "")
	}()
	time.Sleep(1 * time.Second)
}

func makeRequest(tenantID string, isAdmin bool, filters map[string]string) *http.Response {
	tenantUUID, _ := uuid.FromString(tenantID)
	reqID, _ := uuid.NewV4()
	req := Request{
		Context: RequestContext{
			Type:      "port",
			Operation: "READALL",
			TenantID:  tenantUUID,
			RequestID: fmt.Sprintf("req-%s", reqID),
			IsAdmin:   isAdmin,
		},
		Data: RequestData{
			Filters: filters,
		},
	}
	reqJSON, _ := json.Marshal(req)
	resp, _ := http.Post("http://localhost:8080/neutron/port", "application/json", bytes.NewReader(reqJSON))
	return resp
}

func parseBody(resp *http.Response) (ports []neutron.Port) {
	body, _ := ioutil.ReadAll(resp.Body)
	err := json.Unmarshal(body, &ports)
	if err != nil {
		panic(fmt.Sprintf("%s: %s", string(body), err))
	}
	return ports
}

func TestListUser(t *testing.T) {
	start()

	resp := makeRequest(tenantID, false, map[string]string{})
	assert.Equal(t, 200, resp.StatusCode, "")

	ports := parseBody(resp)
	assert.Equal(t, 6, len(ports))

	stop()
}

func TestListUserFilterID(t *testing.T) {
	start()

	resp := makeRequest(tenantID, false, map[string]string{
		"id": "ec12373a-7452-4a51-af9c-5cd9cfb48513",
	})
	assert.Equal(t, 200, resp.StatusCode, "")

	ports := parseBody(resp)
	assert.Equal(t, 1, len(ports))
	assert.Equal(t, "ec12373a-7452-4a51-af9c-5cd9cfb48513", ports[0].ID.String())

	stop()
}

func TestListUserFilterName(t *testing.T) {
	start()

	resp := makeRequest(tenantID, false, map[string]string{
		"name": "aap_vm2_port",
	})
	assert.Equal(t, 200, resp.StatusCode, "")

	ports := parseBody(resp)
	assert.Equal(t, 1, len(ports))
	assert.Equal(t, "aap_vm2_port", ports[0].Name)

	stop()
}

func TestListUserFilterNames(t *testing.T) {
	start()

	resp := makeRequest(tenantID, false, map[string]string{
		"name": "aap_vm1_port,aap_vm2_port",
	})
	assert.Equal(t, 200, resp.StatusCode, "")

	ports := parseBody(resp)
	assert.Equal(t, 2, len(ports))

	stop()
}

func TestListUserFilterVMs(t *testing.T) {
	start()

	resp := makeRequest(tenantID, false, map[string]string{
		"device_id": "bb68ae24-8b17-42b8-86a3-74c99f937b30,31ca7629-5b57-42b7-978b-5c767b24b4b2",
	})
	assert.Equal(t, 200, resp.StatusCode, "")

	ports := parseBody(resp)
	assert.Equal(t, 2, len(ports))

	stop()
}

func TestListUserFilterNetwork(t *testing.T) {
	start()

	resp := makeRequest(tenantID, false, map[string]string{
		"network_id": "e863c27f-ae81-4c0c-926d-28a95ef8b21f",
	})
	assert.Equal(t, 200, resp.StatusCode, "")

	ports := parseBody(resp)
	assert.Equal(t, 4, len(ports))

	stop()
}

func TestAdminList(t *testing.T) {
	start()

	resp := makeRequest(tenantID, true, map[string]string{})
	assert.Equal(t, 200, resp.StatusCode, "")

	ports := parseBody(resp)
	assert.Equal(t, 100, len(ports))

	stop()
}
