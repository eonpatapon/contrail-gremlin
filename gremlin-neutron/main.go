package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"

	"github.com/eonpatapon/contrail-gremlin/utils"
	"github.com/eonpatapon/gremlin"
	cli "github.com/jawher/mow.cli"
	logging "github.com/op/go-logging"
	"github.com/satori/go.uuid"
)

var (
	log    = logging.MustGetLogger(os.Args[0])
	client *gremlin.Client
	quit   = make(chan bool, 1)
	closed = make(chan bool, 1)
)

const (
	READ = iota
	READALL
	UPDATE
	CREATE
	DELETE
)

type RequestContext struct {
	Type      string    `json:"type"`
	Operation string    `json:"operation"`
	TenantID  uuid.UUID `json:"tenant_id"`
	UserID    uuid.UUID `json:"user_id"`
	RequestID string    `json:"request_id"`
	IsAdmin   bool      `json:"is_admin"`
}

type RequestData struct {
	ID      uuid.UUID         `json:"id"`
	Fields  []string          `json:"fields"`
	Filters map[string]string `json:"filters"`
}

type Request struct {
	Context RequestContext
	Data    RequestData
}

var routes = map[string]func(Request) ([]byte, error){
	"READALL_port": listPorts,
}

func globalHandler(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		fmt.Printf("%s", err)
	} else {
		var req Request
		json.Unmarshal(body, &req)
		log.Debugf("%+v\n", req)
		if handler, ok := routes[fmt.Sprintf("%s_%s", req.Context.Operation, req.Context.Type)]; ok {
			res, err := handler(req)
			if err != nil {
				w.WriteHeader(500)
				w.Header().Set("Content-Type", "text/plain; charset=utf-8")
				w.Write([]byte(fmt.Sprintf("%s", err)))
			} else {
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				w.Write(res)
			}
		}
	}
}

func main() {
	app := cli.App(os.Args[0], "")
	gremlinSrv := app.String(cli.StringOpt{
		Name:   "gremlin",
		Value:  "localhost:8182",
		Desc:   "host:port of gremlin server",
		EnvVar: "GREMLIN_NEUTRON_GREMLIN_SERVER",
	})
	utils.SetupLogging(app, log)
	app.Action = func() {
		gremlinURI := fmt.Sprintf("ws://%s/gremlin", *gremlinSrv)
		run(gremlinURI)
	}
	app.Run(os.Args)
}

func stop() {
	quit <- true
	<-closed
}

func run(gremlinURI string) {
	client = gremlin.NewClient(gremlinURI)
	client.Connect()
	log.Info("Connected to gremlin-server")

	mux := http.NewServeMux()
	mux.HandleFunc("/neutron/", globalHandler)

	srv := http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	idleConnsClosed := make(chan struct{})
	go func() {
		sigint := make(chan os.Signal, 1)
		signal.Notify(sigint, os.Interrupt)

		select {
		case <-sigint:
		case <-quit:
		}

		// We received an interrupt signal, shut down.
		if err := srv.Shutdown(context.Background()); err != nil {
			// Error from closing listeners, or context timeout:
			log.Infof("HTTP server Shutdown: %v", err)
		} else {
			log.Info("Stopped HTTP server")
		}
		client.Disconnect()
		log.Info("Disconnected from gremlin-server")
		close(idleConnsClosed)
	}()

	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		// Error starting or closing listener:
		log.Infof("HTTP server ListenAndServe: %v", err)
	}

	<-idleConnsClosed
	closed <- true
}
