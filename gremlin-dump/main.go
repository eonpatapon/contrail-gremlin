package main

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	cli "github.com/jawher/mow.cli"
	logging "github.com/op/go-logging"
	"github.com/satori/go.uuid"
	"github.com/willfaught/gockle"

	g "github.com/eonpatapon/contrail-gremlin/gremlin"
	"github.com/eonpatapon/contrail-gremlin/utils"
)

var (
	log = logging.MustGetLogger(os.Args[0])
)

const (
	// Readers numbers of workers reading cassandra resources
	Readers = 10
)

const (
	DumpStart = iota
	ResourceRead
	ResourceWrite
	DuplicateVertex
	DumpEnd
)

type Dump struct {
	session gockle.Session
	backend *g.GsonBackend
	uuids   chan uuid.UUID
	report  chan int64
	wg      *sync.WaitGroup
}

func NewDump(session gockle.Session, output io.Writer) Dump {
	d := Dump{
		session: session,
		backend: g.NewGsonBackend(output),
		uuids:   make(chan uuid.UUID),
		report:  make(chan int64),
		wg:      &sync.WaitGroup{},
	}
	d.backend.Start()
	return d
}

func (d Dump) Start() {
	go d.reportCount()
	for w := 1; w <= Readers; w++ {
		go d.processResource()
	}
	start := time.Now()
	d.report <- DumpStart
	err := d.getResources()
	if err != nil {
		log.Panicf("Dump failed: %s", err)
	}
	end := time.Now().Sub(start)
	d.report <- DumpEnd
	d.wg.Wait()
	d.backend.Stop()
	fmt.Println()
	log.Noticef("Dump done in %0.2fs", end.Seconds())
}

func (d Dump) reportCount() {
	readCount := 0
	writeCount := 0
	duplicateCount := 0

	dumpStatus := `W`

	for c := range d.report {
		switch c {
		case ResourceRead:
			readCount++
		case ResourceWrite:
			writeCount++
		case DuplicateVertex:
			duplicateCount++
		case DumpStart:
			dumpStatus = `R`
		case DumpEnd:
			dumpStatus = `D`
		}
		fmt.Printf("\rProcessing [read:%d write:%d dup:%d] %s",
			readCount, writeCount, duplicateCount, dumpStatus)
	}
}

func (d Dump) processResource() {
	d.wg.Add(1)
	defer d.wg.Done()
	for uuid := range d.uuids {
		vertex, err := utils.GetContrailResource(d.session, uuid)
		if err != nil {
			log.Warningf("%s", err)
		} else {
			d.report <- ResourceRead
			err := d.backend.Create(vertex)
			if err != nil {
				d.report <- DuplicateVertex
			} else {
				d.report <- ResourceWrite
			}
		}
	}
}

func (d Dump) getResources() error {
	defer close(d.uuids)
	err := utils.GetContrailUUIDs(d.session, d.uuids)
	if err != nil {
		return err
	}
	return nil
}

func setup(cassandraCluster []string, filePath string) {
	var (
		session gockle.Session
		err     error
	)

	log.Notice("Connecting to Cassandra...")
	session, err = utils.SetupCassandra(cassandraCluster)
	if err != nil {
		log.Fatalf("Failed to connect to Cassandra: %s", err)
	}
	log.Notice("Connected.")
	defer session.Close()

	f, err := os.Create(filePath)
	if err != nil {
		log.Fatalf("Failed to open file %s: %s", filePath, err)
	}
	defer f.Close()

	d := NewDump(session, f)
	d.Start()
}

func main() {
	app := cli.App(os.Args[0], "Dump Contrail DB to GraphSON file")
	cassandraSrvs := app.Strings(cli.StringsOpt{
		Name:   "cassandra",
		Value:  []string{"localhost"},
		Desc:   "list of host of cassandra nodes, uses CQL port 9042",
		EnvVar: "GREMLIN_DUMP_CASSANDRA_SERVERS",
	})
	filePath := app.String(cli.StringArg{
		Name: "DST",
		Desc: "Output file path",
	})
	utils.SetupLogging(app, log)
	app.Action = func() {
		setup(*cassandraSrvs, *filePath)
	}
	app.Run(os.Args)
}
