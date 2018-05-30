package main

import (
	"os"
	"testing"
	"time"

	"github.com/eonpatapon/contrail-gremlin/testutils"
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

func start() {
	go func() {
		run("ws://localhost:8182/gremlin", "", "n")
	}()
	time.Sleep(1 * time.Second)
}
