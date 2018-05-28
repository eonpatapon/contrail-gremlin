package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"testing"
	"time"

	"github.com/akutz/gotil"
)

var tenantID = "0ed483e083ef4f7082501fcfa5d98c0e"

func TestMain(m *testing.M) {
	cmd := startGremlinServerWithDump("gremlin-neutron.yml", "2305.json")
	start()
	res := m.Run()
	stop()
	stopGremlinServer(cmd)
	os.Exit(res)
}

func start() {
	go func() {
		run("ws://localhost:8182/gremlin", "")
	}()
	time.Sleep(1 * time.Second)
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}
	return out.Close()
}

func rootDir() string {
	cwd, err := os.Getwd()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to get current working dir:", err)
		os.Exit(1)
	}
	return path.Dir(cwd)
}

func dumpPath(dumpName string) string {
	cwd := rootDir()
	return cwd + "/resources/dumps/" + dumpName
}

func startGremlinServerWithDump(confFile string, dumpName string) *exec.Cmd {
	copyFile(dumpPath(dumpName), "/tmp/dump.json")
	return startGremlinServer(confFile)
}

// StartGremlinServer starts the gremlin-server
func startGremlinServer(confFile string) *exec.Cmd {
	gremlinServerPath := os.Getenv("GREMLIN_SERVER")
	if gremlinServerPath == "" {
		fmt.Fprintln(os.Stderr, "GREMLIN_SERVER env variable not set")
		os.Exit(1)
	}
	cwd := rootDir()
	for _, file := range []string{
		"conf/gremlin-contrail.properties",
		"conf/gremlin-contrail.yml",
		"conf/gremlin-neutron.properties",
		"conf/gremlin-neutron.yml",
		"scripts/gremlin-contrail.groovy",
	} {
		src := fmt.Sprintf("%s/resources/%s", cwd, file)
		dst := fmt.Sprintf("%s/%s", gremlinServerPath, file)
		err := copyFile(src, dst)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Failed to copy conf", src, "to", dst)
			os.Exit(1)
		}
	}
	cmd := exec.Command("/bin/sh", "bin/gremlin-server.sh", fmt.Sprintf("conf/%s", confFile))
	cmd.Dir = gremlinServerPath
	cmdReader, err := cmd.StdoutPipe()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error creating StdoutPipe for Cmd:", err)
		os.Exit(1)
	}
	scanner := bufio.NewScanner(cmdReader)
	go func() {
		for scanner.Scan() {
			fmt.Println(scanner.Text())
		}
	}()
	err = cmd.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to start process:", err)
		os.Exit(1)
	}
	for gotil.IsTCPPortAvailable(8182) {
		time.Sleep(1 * time.Second)
	}
	time.Sleep(3 * time.Second)
	return cmd
}

func stopGremlinServer(cmd *exec.Cmd) error {
	if err := cmd.Process.Kill(); err != nil {
		fmt.Fprintln(os.Stderr, "Failed to kill process:", err)
		os.Exit(1)
	}
	return cmd.Wait()
}
