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

// RequestContext the context of incoming requests
type RequestContext struct {
	Type      string    `json:"type"`
	Operation string    `json:"operation"`
	TenantID  uuid.UUID `json:"tenant_id"`
	UserID    uuid.UUID `json:"user_id"`
	RequestID string    `json:"request_id"`
	IsAdmin   bool      `json:"is_admin"`
}

// RequestData the data of incoming requests
type RequestData struct {
	ID      uuid.UUID                `json:"id"`
	Fields  []string                 `json:"fields"`
	Filters map[string][]interface{} `json:"filters"`
}

// Request the incoming request from neutron plugin
type Request struct {
	Context RequestContext
	Data    RequestData
}

// App the context shared by concurrent requests
type App struct {
	gremlinClient  *gremlin.Client
	contrailClient *http.Client
	contrailAPIURL string
	quit           chan bool
	closed         chan bool
	methods        map[string]func(Request) ([]byte, error)
}

func newApp(gremlinURI string, contrailAPISrv string) *App {
	a := &App{
		contrailAPIURL: fmt.Sprintf("http://%s", contrailAPISrv),
		contrailClient: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
	a.methods = map[string]func(Request) ([]byte, error){
		"READALL_port":    a.listPorts,
		"READALL_network": a.listNetworks,
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

func copyHeaders(src, dst http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

func (a *App) forward(w http.ResponseWriter, r *http.Request, body io.Reader) {
	url := a.contrailAPIURL + r.URL.Path
	log.Debugf("Forwarding to %s", url)
	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		log.Error(err.Error())
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	copyHeaders(r.Header, req.Header)
	resp, err := a.contrailClient.Do(req)
	if err != nil {
		log.Error(err.Error())
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	defer resp.Body.Close()
	copyHeaders(resp.Header, w.Header())
	log.Debugf("Code: %d", resp.StatusCode)
	w.WriteHeader(resp.StatusCode)
	_, err = io.Copy(w, resp.Body)
	if err != nil {
		log.Errorf("Failed to copy response data")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (a *App) handler(w http.ResponseWriter, r *http.Request) {
	if !a.gremlinClient.IsConnected() {
		a.forward(w, r, r.Body)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Errorf("Failed to read request: %s", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var req Request
	if err := json.Unmarshal(body, &req); err != nil {
		log.Errorf("Failed to parse request %s: %s", string(body), err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	log.Debugf("Request: %+v\n", req)

	// Check if we have an implementation for this request
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
		log.Debugf("Response: %s", string(res))
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Write(res)
	} else {
		a.forward(w, r, bytes.NewReader(body))
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
	utils.SetupLogging(app, log)
	app.Action = func() {
		gremlinURI := fmt.Sprintf("ws://%s/gremlin", *gremlinSrv)
		run(gremlinURI, *contrailAPISrv)
	}
	app.Run(os.Args)
}

func stop() {
	quit <- true
	<-closed
}

func run(gremlinURI string, contrailAPISrv string) {

	app := newApp(gremlinURI, contrailAPISrv)

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
