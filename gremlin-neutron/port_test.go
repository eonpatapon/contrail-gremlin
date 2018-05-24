package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/eonpatapon/contrail-gremlin/lib"
	"github.com/eonpatapon/contrail-gremlin/neutron"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

var tenantID = "0ed483e083ef4f7082501fcfa5d98c0e"

func start() {
	go func() {
		run("ws://localhost:8182/gremlin", "")
	}()
	time.Sleep(1 * time.Second)
}

func makeRequest(tenantID string, isAdmin bool, data RequestData) *http.Response {
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
		Data: data,
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

func TestMain(m *testing.M) {
	cmd := lib.StartGremlinServerWithDump("gremlin-neutron.yml", "2305.json")
	start()
	res := m.Run()
	stop()
	lib.StopGremlinServer(cmd)
	os.Exit(res)
}

func TestListUser(t *testing.T) {
	resp := makeRequest(tenantID, false, RequestData{})
	assert.Equal(t, 200, resp.StatusCode, "")

	ports := parseBody(resp)
	assert.Equal(t, 6, len(ports))
}

func TestListUserFilterID(t *testing.T) {
	resp := makeRequest(tenantID, false, RequestData{
		Filters: map[string][]string{
			"id": []string{"ec12373a-7452-4a51-af9c-5cd9cfb48513"},
		},
	})
	assert.Equal(t, 200, resp.StatusCode, "")

	ports := parseBody(resp)
	assert.Equal(t, 1, len(ports))
	assert.Equal(t, "ec12373a-7452-4a51-af9c-5cd9cfb48513", ports[0].ID.String())
}

func TestListUserFilterName(t *testing.T) {
	resp := makeRequest(tenantID, false, RequestData{
		Filters: map[string][]string{
			"name": []string{"aap_vm2_port"},
		},
	})
	assert.Equal(t, 200, resp.StatusCode, "")

	ports := parseBody(resp)
	assert.Equal(t, 1, len(ports))
	assert.Equal(t, "aap_vm2_port", ports[0].Name)
}

func TestListUserFilterNames(t *testing.T) {
	resp := makeRequest(tenantID, false, RequestData{
		Filters: map[string][]string{
			"name": []string{"aap_vm1_port", "aap_vm2_port"},
		},
	})
	assert.Equal(t, 200, resp.StatusCode, "")

	ports := parseBody(resp)
	assert.Equal(t, 2, len(ports))
}

func TestListUserFilterVMs(t *testing.T) {
	resp := makeRequest(tenantID, false, RequestData{
		Filters: map[string][]string{
			"device_id": []string{"bb68ae24-8b17-42b8-86a3-74c99f937b30", "31ca7629-5b57-42b7-978b-5c767b24b4b2"},
		},
	})
	assert.Equal(t, 200, resp.StatusCode, "")

	ports := parseBody(resp)
	assert.Equal(t, 2, len(ports))
}

func TestListUserFilterNetwork(t *testing.T) {
	resp := makeRequest(tenantID, false, RequestData{
		Filters: map[string][]string{
			"network_id": []string{"e863c27f-ae81-4c0c-926d-28a95ef8b21f"},
		},
	})
	assert.Equal(t, 200, resp.StatusCode, "")

	ports := parseBody(resp)
	assert.Equal(t, 4, len(ports))
}

func TestListUserFilterIPAddress(t *testing.T) {
	resp := makeRequest(tenantID, false, RequestData{
		Filters: map[string][]string{
			"fixed_ips": []string{"ip_address=15.15.15.5"},
		},
	})
	assert.Equal(t, 200, resp.StatusCode, "")

	ports := parseBody(resp)
	assert.Equal(t, 1, len(ports))
}

func TestListUserFilterSubnetID(t *testing.T) {
	resp := makeRequest(tenantID, false, RequestData{
		Filters: map[string][]string{
			"fixed_ips": []string{"subnet_id=04613d72-cae0-4cf1-83c6-327d163e238d"},
		},
	})
	assert.Equal(t, 200, resp.StatusCode, "")

	ports := parseBody(resp)
	assert.Equal(t, 2, len(ports))
}

func TestListAdmin(t *testing.T) {
	resp := makeRequest(tenantID, true, RequestData{})
	assert.Equal(t, 200, resp.StatusCode, "")

	ports := parseBody(resp)
	assert.Equal(t, 107, len(ports))
}

func TestListUserFields(t *testing.T) {
	resp := makeRequest(tenantID, false, RequestData{
		Fields: []string{"id", "mac_address"},
	})
	assert.Equal(t, 200, resp.StatusCode, "")
	ports := parseBody(resp)
	assert.Equal(t, "", ports[0].Status)
	assert.Equal(t, "", ports[0].DeviceID)
}
