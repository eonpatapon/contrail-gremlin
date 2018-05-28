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

func makeNetworkRequest(tenantID string, isAdmin bool, data RequestData) *http.Response {
	tenantUUID, _ := uuid.FromString(tenantID)
	reqID, _ := uuid.NewV4()
	req := Request{
		Context: RequestContext{
			Type:      "network",
			Operation: "READALL",
			TenantID:  tenantUUID,
			RequestID: fmt.Sprintf("req-%s", reqID),
			IsAdmin:   isAdmin,
		},
		Data: data,
	}
	reqJSON, _ := json.Marshal(req)
	resp, _ := http.Post("http://localhost:8080/neutron/network", "application/json", bytes.NewReader(reqJSON))
	return resp
}

func parseNetworks(resp *http.Response) (networks []neutron.Network) {
	body, _ := ioutil.ReadAll(resp.Body)
	err := json.Unmarshal(body, &networks)
	if err != nil {
		panic(fmt.Sprintf("%s: %s", string(body), err))
	}
	return networks
}

func TestNetworkListUser(t *testing.T) {
	resp := makeNetworkRequest(tenantID, false, RequestData{})
	assert.Equal(t, 200, resp.StatusCode, "")

	nets := parseNetworks(resp)
	assert.Equal(t, 3, len(nets))
}
