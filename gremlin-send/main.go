package main

import (
	"fmt"
	"os"

	"github.com/eonpatapon/contrail-gremlin/utils"
	"github.com/eonpatapon/gremlin"
	cli "github.com/jawher/mow.cli"
	logging "github.com/op/go-logging"
)

var (
	log = logging.MustGetLogger("gremlin-send")
)

func main() {
	app := cli.App(os.Args[0], "Send script to gremlin-server")
	gremlinSrv := app.String(cli.StringOpt{
		Name:   "gremlin",
		Value:  "localhost:8182",
		Desc:   "host:port of gremlin server",
		EnvVar: "GREMLIN_SEND_GREMLIN_SERVER",
	})
	script := app.String(cli.StringArg{
		Name: "SCRIPT",
		Desc: "gremlin script",
	})
	utils.SetupLogging(app, log)
	app.Action = func() {
		if err := gremlin.NewCluster(fmt.Sprintf("ws://%s/gremlin", *gremlinSrv)); err != nil {
			log.Fatal("Failed to setup gremlin server.")
		}

		data, err := gremlin.Query(*script).Exec()
		if err != nil {
			log.Fatalf("Error while sending script: %s", err.Error())
		}
		fmt.Println(string(data))
	}
	app.Run(os.Args)
}
