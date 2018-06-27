package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/eonpatapon/contrail-gremlin/neutron"
	"github.com/stretchr/testify/assert"
)

func makeNetworkRequest(tenantID string, isAdmin bool, data RequestData) *http.Response {
	return makeRequest("network", ListRequest, tenantID, isAdmin, data)
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
	assert.Equal(t, 4, len(nets))
}
