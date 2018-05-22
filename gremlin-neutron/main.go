package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/eonpatapon/contrail-gremlin/utils"
	"github.com/eonpatapon/gremlin"
	cli "github.com/jawher/mow.cli"
	logging "github.com/op/go-logging"
	"github.com/satori/go.uuid"
)

var (
	log    = logging.MustGetLogger(os.Args[0])
	quit   = make(chan bool, 1)
	closed = make(chan bool, 1)
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
	ID      uuid.UUID           `json:"id"`
	Fields  []string            `json:"fields"`
	Filters map[string][]string `json:"filters"`
}

type Request struct {
	Context RequestContext
	Data    RequestData
}

type App struct {
	gremlinClient       *gremlin.Client
	contrailClient      *http.Client
	contrailAPIURL      string
	contrailAPIUser     string
	contrailAPIPassword string
	quit                chan bool
	closed              chan bool
	methods             map[string]func(Request) ([]byte, error)
}

func NewApp(gremlinURI string, contrailAPISrv string, contrailAPIUser string, contrailAPIPassword string) *App {
	a := &App{
		contrailAPIURL: fmt.Sprintf("http://%s", contrailAPISrv),
		contrailClient: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
	a.methods = map[string]func(Request) ([]byte, error){
		"READALL_port": a.listPorts,
	}
	a.gremlinClient = gremlin.NewClient(gremlinURI)
	a.gremlinClient.AddConnectedHandler(a.onGremlinConnect)
	a.gremlinClient.AddDisconnectedHandler(a.onGremlinDisconnect)
	a.gremlinClient.ConnectAsync()
	return a
}

func (a *App) onGremlinConnect() {
	log.Notice("Connected to gremlin-server")
}

func (a *App) onGremlinDisconnect(err error) {
	if err != nil {
		log.Warningf("Disconnected from gremlin-server: %s", err)
	} else {
		log.Notice("Disconnected from gremlin-server")
	}
}

func copyHeader(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

func (a *App) forward(w http.ResponseWriter, path string, body io.Reader) {
	req, err := http.NewRequest("POST", a.contrailAPIURL+path, body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	req.SetBasicAuth(a.contrailAPIUser, a.contrailAPIPassword)
	req.Header.Add("Content-Type", "application/json")
	resp, err := a.contrailClient.Do(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	defer resp.Body.Close()
	copyHeader(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func (a *App) handler(w http.ResponseWriter, r *http.Request) {
	if !a.gremlinClient.IsConnected() {
		a.forward(w, r.URL.Path, r.Body)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Errorf("Failed to read request: %s", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req Request
	json.Unmarshal(body, &req)
	log.Debugf("%+v\n", req)

	handler, ok := a.methods[fmt.Sprintf("%s_%s", req.Context.Operation, req.Context.Type)]

	if ok {
		res, err := handler(req)
		if err != nil {
			log.Errorf("Handler hit an error: %s", err)
			w.WriteHeader(500)
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.Write([]byte(fmt.Sprintf("%s", err)))
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Write(res)
	} else {
		a.forward(w, r.URL.Path, bytes.NewReader(body))
	}
}

func (a *App) stop() {
	a.gremlinClient.Disconnect()
}

func main() {
	app := cli.App(os.Args[0], "")
	gremlinSrv := app.String(cli.StringOpt{
		Name:   "gremlin",
		Value:  "localhost:8182",
		Desc:   "host:port of gremlin server",
		EnvVar: "GREMLIN_NEUTRON_GREMLIN_SERVER",
	})
	contrailAPISrv := app.String(cli.StringOpt{
		Name:   "contrail-api",
		Value:  "localhost:8082",
		Desc:   "host:port of contrail-api server",
		EnvVar: "GREMLIN_NEUTRON_CONTRAIL_API_SERVER",
	})
	contrailAPIUser := app.String(cli.StringOpt{
		Name:   "contrail-api-user",
		EnvVar: "GREMLIN_NEUTRON_CONTRAIL_API_USER",
	})
	contrailAPIPassword := app.String(cli.StringOpt{
		Name:   "contrail-api-password",
		EnvVar: "GREMLIN_NEUTRON_CONTRAIL_API_PASSWORD",
	})
	utils.SetupLogging(app, log)
	app.Action = func() {
		gremlinURI := fmt.Sprintf("ws://%s/gremlin", *gremlinSrv)
		run(gremlinURI, *contrailAPISrv, *contrailAPIUser, *contrailAPIPassword)
	}
	app.Run(os.Args)
}

func stop() {
	quit <- true
	<-closed
}

func run(gremlinURI string, contrailAPISrv string, contrailAPIUser string, contrailAPIPassword string) {

	app := NewApp(gremlinURI, contrailAPISrv, contrailAPIUser, contrailAPIPassword)

	mux := http.NewServeMux()
	mux.HandleFunc("/neutron/", app.handler)

	srv := http.Server{
		Addr:         ":8080",
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 25 * time.Second,
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
			log.Errorf("HTTP server shutdown error: %s", err)
		} else {
			log.Notice("Stopped HTTP server")
		}
		close(idleConnsClosed)
	}()

	log.Notice("Starting HTTP server...")
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Errorf("HTTP server error: %s", err)
	}

	<-idleConnsClosed
	app.stop()
	closed <- true
}
