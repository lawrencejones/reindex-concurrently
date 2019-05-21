package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/alecthomas/kingpin"
	kitlog "github.com/go-kit/kit/log"
	"github.com/jackc/pgx"
)

var logger kitlog.Logger

var (
	app                = kingpin.New("reindex-concurrently", "").Version("0.0.1")
	index              = app.Arg("index", "name of index").Required().String()
	host               = app.Flag("host", "PostgreSQL database host").Default("/var/run/postgresql").String()
	port               = app.Flag("port", "PostgreSQL database port").Default("5432").Uint16()
	dbname             = app.Flag("dbname", "PostgreSQL root database").Required().String()
	user               = app.Flag("user", "PostgreSQL user").Default("postgres").String()
	lockTimeout        = app.Flag("lock-timeout", "timeout for acquiring access exclusive lock").Default("250ms").String()
	maintenanceWorkMem = app.Flag("maintenance-work-mem", "session maintenance work mem").Default("1GB").String()
)

func main() {
	logger = kitlog.NewLogfmtLogger(kitlog.NewSyncWriter(os.Stderr))
	logger = kitlog.With(logger, "ts", kitlog.DefaultTimestampUTC, "caller", kitlog.DefaultCaller)

	kingpin.MustParse(app.Parse(os.Args[1:]))

	logger.Log("msg", "connecting to database", "dbname", *dbname, "host", *host, "port", *port, "user", *user)
	conn, err := pgx.Connect(
		pgx.ConnConfig{
			Host:     *host,
			Port:     *port,
			Database: *dbname,
			User:     *user,
		},
	)

	if err != nil {
		kingpin.Fatalf("failed to connect to database: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigs := make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGTERM)
	go func() {
		<-sigs
		logger.Log("msg", "received signal, shutting down")
		cancel()
	}()

	logger.Log("msg", "setting lock timeout", "lock_timeout", *lockTimeout)
	_, err = conn.ExecEx(ctx, fmt.Sprintf(`set lock_timeout = '%s';`, *lockTimeout), nil)
	if err != nil {
		panic(err)
	}

	logger.Log("msg", "setting maintenance work mem", "maintenance_work_mem", *maintenanceWorkMem)
	_, err = conn.ExecEx(ctx, fmt.Sprintf(`set maintenance_work_mem = '%s';`, *maintenanceWorkMem), nil)
	if err != nil {
		panic(err)
	}

	indexDef, oldSize, err := getIndex(ctx, conn, *index)
	if err != nil {
		panic(err)
	}

	workingIndex := fmt.Sprintf("%s_working", *index)
	workingIndexDef := strings.Replace(indexDef, *index, workingIndex, 1)

	start := time.Now()
	logger.Log("msg", "creating new index", "index", workingIndex, "definition", workingIndexDef)
	_, err = conn.ExecEx(ctx, workingIndexDef, nil)
	if err != nil {
		panic(err)
	}

	logger.Log("msg", "successfully created new index", "index", workingIndex, "duration", time.Since(start).Seconds())

	logger.Log("msg", "dropping old index", "index", *index)
	_, err = conn.ExecEx(ctx, fmt.Sprintf(`drop index concurrently %s;`, *index), nil)
	if err != nil {
		panic(err)
	}

	logger.Log("msg", "rename new index to old", "from", workingIndex, "to", *index)
	_, err = conn.ExecEx(ctx, fmt.Sprintf(`alter index %s rename to %s;`, workingIndex, *index), nil)
	if err != nil {
		panic(err)
	}

	_, newSize, err := getIndex(ctx, conn, *index)
	if err != nil {
		panic(err)
	}

	logger.Log("msg", "done", "index", *index, "old_size", oldSize, "new_size", newSize)
}

func getIndex(ctx context.Context, conn *pgx.Conn, index string) (def string, size string, err error) {
	rows, err := conn.QueryEx(ctx, `select indexdef, pg_size_pretty(pg_total_relation_size(indexname::text)) from pg_indexes where indexname = $1;`, nil, index)
	if err != nil {
		return "", "", err
	}

	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&def, &size)
		if err != nil {
			return "", "", err
		}

		return def, size, err
	}

	return "", "", fmt.Errorf("failed to find index '%s'", index)
}
