package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/eonpatapon/contrail-gremlin/neutron"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func makePortRequest(tenantID string, isAdmin bool, data RequestData) *http.Response {
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

func parsePorts(resp *http.Response) (ports []neutron.Port) {
	body, _ := ioutil.ReadAll(resp.Body)
	err := json.Unmarshal(body, &ports)
	if err != nil {
		panic(fmt.Sprintf("%s: %s", string(body), err))
	}
	return ports
}

func TestListUser(t *testing.T) {
	resp := makePortRequest(tenantID, false, RequestData{})
	assert.Equal(t, 200, resp.StatusCode, "")

	ports := parsePorts(resp)
	assert.Equal(t, 6, len(ports))
}

func TestUserAAP(t *testing.T) {
	resp := makePortRequest(tenantID, false, RequestData{
		Filters: RequestFilters{
			"name": []interface{}{"aap_vm1_port"},
		},
	})
	assert.Equal(t, 200, resp.StatusCode, "")

	ports := parsePorts(resp)
	assert.Equal(t, 1, len(ports))
	assert.Equal(t, "15.15.15.15", ports[0].AAPs[0].IP)
	assert.Equal(t, "00:00:5e:00:01:33", ports[0].AAPs[0].MAC)
}

func TestListUserFilterID(t *testing.T) {
	resp := makePortRequest(tenantID, false, RequestData{
		Filters: RequestFilters{
			"id": []interface{}{"ec12373a-7452-4a51-af9c-5cd9cfb48513"},
		},
	})
	assert.Equal(t, 200, resp.StatusCode, "")

	ports := parsePorts(resp)
	assert.Equal(t, 1, len(ports))
	assert.Equal(t, "ec12373a-7452-4a51-af9c-5cd9cfb48513", ports[0].ID.String())
}

func TestListUserFilterName(t *testing.T) {
	resp := makePortRequest(tenantID, false, RequestData{
		Filters: RequestFilters{
			"name": []interface{}{"aap_vm2_port"},
		},
	})
	assert.Equal(t, 200, resp.StatusCode, "")

	ports := parsePorts(resp)
	assert.Equal(t, 1, len(ports))
	assert.Equal(t, "aap_vm2_port", ports[0].Name)
}

func TestListUserFilterNames(t *testing.T) {
	resp := makePortRequest(tenantID, false, RequestData{
		Filters: RequestFilters{
			"name": []interface{}{"aap_vm1_port", "aap_vm2_port"},
		},
	})
	assert.Equal(t, 200, resp.StatusCode, "")

	ports := parsePorts(resp)
	assert.Equal(t, 2, len(ports))
}

func TestListUserFilterVMs(t *testing.T) {
	resp := makePortRequest(tenantID, false, RequestData{
		Filters: RequestFilters{
			"device_id": []interface{}{"bb68ae24-8b17-42b8-86a3-74c99f937b30", "31ca7629-5b57-42b7-978b-5c767b24b4b2"},
		},
	})
	assert.Equal(t, 200, resp.StatusCode, "")

	ports := parsePorts(resp)
	assert.Equal(t, 2, len(ports))
}

func TestListUserFilterNetwork(t *testing.T) {
	resp := makePortRequest(tenantID, false, RequestData{
		Filters: RequestFilters{
			"network_id": []interface{}{"e863c27f-ae81-4c0c-926d-28a95ef8b21f"},
		},
	})
	assert.Equal(t, 200, resp.StatusCode, "")

	ports := parsePorts(resp)
	assert.Equal(t, 4, len(ports))
}

func TestListUserFilterIPAddress(t *testing.T) {
	resp := makePortRequest(tenantID, false, RequestData{
		Filters: RequestFilters{
			"ip_address": []interface{}{"15.15.15.5"},
		},
	})
	assert.Equal(t, 200, resp.StatusCode, "")

	ports := parsePorts(resp)
	assert.Equal(t, 1, len(ports))
}

func TestListUserFilterSubnetID(t *testing.T) {
	resp := makePortRequest(tenantID, false, RequestData{
		Filters: RequestFilters{
			"subnet_id": []interface{}{"04613d72-cae0-4cf1-83c6-327d163e238d"},
		},
	})
	assert.Equal(t, 200, resp.StatusCode, "")

	ports := parsePorts(resp)
	assert.Equal(t, 2, len(ports))
}

func TestListAdmin(t *testing.T) {
	resp := makePortRequest(tenantID, true, RequestData{})
	assert.Equal(t, 200, resp.StatusCode, "")

	ports := parsePorts(resp)
	assert.Equal(t, 107, len(ports))
}

func TestListUserFields(t *testing.T) {
	resp := makePortRequest(tenantID, false, RequestData{
		Fields: []string{"id", "mac_address"},
	})
	assert.Equal(t, 200, resp.StatusCode, "")
	ports := parsePorts(resp)
	assert.Equal(t, "", ports[0].Status)
	assert.Equal(t, "", ports[0].DeviceID)
}
