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
	"github.com/jackc/pgx/pgtype"
)

var logger kitlog.Logger

var (
	app                           = kingpin.New("reindex-concurrently", "").Version("0.0.1")
	index                         = app.Arg("index", "name of index").Required().String()
	host                          = app.Flag("host", "PostgreSQL database host").Default("/var/run/postgresql").String()
	port                          = app.Flag("port", "PostgreSQL database port").Default("5432").Uint16()
	dbname                        = app.Flag("dbname", "PostgreSQL root database").Required().String()
	user                          = app.Flag("user", "PostgreSQL user").Default("postgres").String()
	renameIndexRetries            = app.Flag("rename-index-retries", "Number of times to try renaming index").Default("10").Int()
	renameIndexRetryInterval      = app.Flag("rename-index-retry-interval", "Time between rename index retries").Default("30s").Duration()
	synchronousCommit             = app.Flag("synchronous-commit", "synchronous_commit setting for index build").Default("on").String()
	temporaryFileLimit            = app.Flag("temporary-file-limit", "limit on temporary files").Default("100GB").String()
	maxParallelMaintenanceWorkers = app.Flag("max-parallel-maintenance-workers", "number of parallel index workers").Default("4").String()
	lockTimeout                   = app.Flag("lock-timeout", "timeout for acquiring access exclusive lock").Default("3s").String()
	maintenanceWorkMem            = app.Flag("maintenance-work-mem", "session maintenance work mem").Default("1GB").String()
)

const (
	selectIndexByName = `
select pg_class_index.relname as name
     , pg_class_index.oid as oid
     , pg_class_table.relname as table
     , pg_get_indexdef(pg_class_index.oid) as definition
     , pg_size_pretty(pg_total_relation_size(pg_class_index.relname::text))
  from pg_index
       join pg_class pg_class_table on pg_index.indrelid = pg_class_table.oid
       join pg_class pg_class_index on pg_index.indexrelid = pg_class_index.oid
       left join pg_namespace on pg_namespace.oid = pg_class_table.relnamespace
 where nspname != 'pg_toast'
   and pg_class_index.relname = $1;
	`
	selectIndexConstraintsByOid = `
select conname as name
     , pg_class_table.relname as table
     , pg_get_constraintdef(pg_constraint.oid) as definition
     , pg_constraint.contype as check_type
  from pg_constraint
       join pg_class pg_class_table on pg_constraint.conrelid = pg_class_table.oid
 where conindid = $1;
	`
	dropConstraint            = `alter table %s drop constraint %s;`
	alterConstraintUsingIndex = `alter table %s drop constraint %s, add constraint %s %s using index %s;`
	addConstraintNotValid     = `alter table %s add constraint %s %s not valid;`
	validateConstraint        = `alter table %s validate constraint %s;`
)

func main() {
	logger = kitlog.NewLogfmtLogger(kitlog.NewSyncWriter(os.Stderr))
	logger = kitlog.With(logger, "ts", kitlog.DefaultTimestampUTC)

	kingpin.MustParse(app.Parse(os.Args[1:]))

	logger.Log("msg", "connecting to database", "dbname", *dbname, "host", *host, "port", *port, "user", *user)
	conn, err := pgx.Connect(
		pgx.ConnConfig{
			Host:     *host,
			Port:     *port,
			Database: *dbname,
			User:     *user,
			RuntimeParams: map[string]string{
				"application_name": "reindex-concurrently",
			},
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

	mustSet(ctx, conn, "maintenance_work_mem", *maintenanceWorkMem)
	mustSet(ctx, conn, "temp_file_limit", *temporaryFileLimit)
	mustSet(ctx, conn, "synchronous_commit", *synchronousCommit)
	mustSet(ctx, conn, "max_parallel_maintenance_workers", *maxParallelMaintenanceWorkers)

	idx, err := getIndex(ctx, conn, *index)
	if err != nil {
		panic(err)
	}

	workingIndex := fmt.Sprintf("%s_working", idx.name)
	workingIndexDef := strings.Replace(idx.definition, idx.name, workingIndex, 1)
	workingIndexDef = strings.Replace(workingIndexDef, `INDEX`, `INDEX CONCURRENTLY`, 1)

	if !strings.Contains(workingIndexDef, workingIndex) {
		kingpin.Fatalf("could not construct an index instruction: %s", workingIndexDef)
	}

	if !strings.Contains(workingIndexDef, `CONCURRENTLY`) {
		kingpin.Fatalf("could not construct a concurrent index definition: %s", workingIndexDef)
	}

	// If index already exists, drop it concurrently. This will be leftover from a
	// previously unsuccessful reindex attempt.
	if _, err := getIndex(ctx, conn, workingIndex); err == nil {
		logger.Log("msg", "dropping previous working index", "index", workingIndex)
		_, err = conn.ExecEx(ctx, fmt.Sprintf(`drop index concurrently %s`, workingIndex), nil)
		if err != nil {
			panic(err)
		}
	}

	start := time.Now()
	logger.Log("msg", "creating new index", "index", workingIndex, "definition", workingIndexDef)
	_, err = conn.ExecEx(ctx, workingIndexDef, nil)
	if err != nil {
		panic(err)
	}

	logger.Log("msg", "successfully created new index", "index", workingIndex, "duration", time.Since(start).Seconds())

	// ALTER TABLE's take AccessExclusive locks on the target relation- ensure we set a
	// timeout for this constraint modification.
	func() {
		mustSet(ctx, conn, "lock_timeout", *lockTimeout)
		defer mustSet(ctx, conn, "lock_timeout", "")

		logger.Log("msg", "beginning transaction")
		txn, err := conn.BeginEx(ctx, nil)
		if err != nil {
			panic(err)
		}

		// Temporarily drop all foreign key constraints that depend on this index. This
		// ensures the rename of the primary key index can occur without raising an error
		// because constraints depend on it.
		for _, c := range selectByType(idx.constraints, ForeignKey) {
			dropQuery := fmt.Sprintf(dropConstraint, c.table, c.name)
			logger.Log("msg", "dropping foreign key constraint", "constraint", c.name, "query", dropQuery)
			_, err = conn.ExecEx(ctx, dropQuery, nil)
			if err != nil {
				panic(err)
			}
		}

		// Any unique constraints need modifying to be powered by the new index. Postgres
		// supports this switch atomically.
		for _, c := range selectByType(idx.constraints, Unique) {
			alterQuery := fmt.Sprintf(alterConstraintUsingIndex, c.table, c.name, c.name, `UNIQUE`, workingIndex)
			logger.Log("msg", "switching constraint to use new index", "constraint", c.name, "query", alterQuery)
			_, err = conn.ExecEx(ctx, alterQuery, nil)
			if err != nil {
				panic(err)
			}
		}

		// After dropping the pesky foreign key constraints we can now alter the primary key
		// index. Postgres supports this atomically and will automatically rename the working
		// index to match whatever the existing index is named.
		for _, c := range selectByType(idx.constraints, PrimaryKey) {
			alterQuery := fmt.Sprintf(alterConstraintUsingIndex, c.table, c.name, c.name, `PRIMARY KEY`, workingIndex)
			logger.Log("msg", "altering primary constraint to use new index", "constraint", c.name, "query", alterQuery)
			_, err = conn.ExecEx(ctx, alterQuery, nil)
			if err != nil {
				panic(err)
			}
		}

		// As the final act before closing our transaction, recreate the constraints we
		// dropped as not valid. This should be immediately but will become active for new
		// tuples as soon as the transaction commits, ensuring we continue to preserve the
		// guarantees from the old constraints.
		for _, c := range selectByType(idx.constraints, ForeignKey) {
			addQuery := fmt.Sprintf(addConstraintNotValid, c.table, c.name, c.definition)
			logger.Log("msg", "re-creating deleted constraint", "constraint", c.name, "query", addQuery)
			_, err = conn.ExecEx(ctx, addQuery, nil)
			if err != nil {
				panic(err)
			}
		}

		logger.Log("msg", "commiting transaction")
		if err := txn.Commit(); err != nil {
			panic(err)
		}

		// Outside our original transaction, now validate the new constraints.
		for _, c := range selectByType(idx.constraints, ForeignKey) {
			validateQuery := fmt.Sprintf(validateConstraint, c.table, c.name)

			logger := kitlog.With(logger, "constraint", c.name)
			logger.Log("msg", "validating constraint", "query", validateQuery)

			start := time.Now()
			_, err = conn.ExecEx(ctx, validateQuery, nil)
			if err != nil {
				panic(err)
			}

			logger.Log("msg", "validated constraint", "duration", time.Since(start).Seconds())
		}
	}()

	// If we have primary constraints we will have dropped and renamed them in the previous
	// flow. Our working index will already have been renamed to match, so we have no need
	// to drop it again.
	if len(selectByType(idx.constraints, PrimaryKey)) == 0 {
		logger.Log("msg", "dropping old index", "index", *index)
		_, err = conn.ExecEx(ctx, fmt.Sprintf(`drop index concurrently %s;`, *index), nil)
		if err != nil {
			panic(err)
		}

		// ALTER INDEX is actually an ALTER TABLE and takes AccessExclusive locks. Apply timeout
		// to prevent sadness!
	renameIndex:
		for remainingAttempts := *renameIndexRetries; remainingAttempts > 0; remainingAttempts-- {
			err := func() error {
				mustSet(ctx, conn, "lock_timeout", *lockTimeout)
				defer mustSet(ctx, conn, "lock_timeout", "")

				logger.Log("msg", "rename new index to old", "from", workingIndex, "to", idx.name)
				_, err = conn.ExecEx(ctx, fmt.Sprintf(`alter index %s rename to %s;`, workingIndex, idx.name), nil)

				return err
			}()

			if err == nil {
				break renameIndex
			}

			if remainingAttempts == 1 {
				panic(err)
			}

			logger.Log("error", err, "msg", "failed to drop index, retrying after pause", "remainingAttempts", remainingAttempts)
			select {
			case <-ctx.Done():
				return
			case <-time.After(*renameIndexRetryInterval):
				// continue
			}
		}
	}

	newIdx, err := getIndex(ctx, conn, idx.name)
	if err != nil {
		panic(err)
	}

	logger.Log("msg", "done", "index", idx.name, "old_size", idx.size, "new_size", newIdx.size)
}

func mustSet(ctx context.Context, conn *pgx.Conn, name, value string) {
	if value == "" {
		logger.Log("event", "pg_setting_reset", "setting", name)
		_, err := conn.ExecEx(ctx, fmt.Sprintf(`reset %s;`, name), nil)
		if err != nil {
			panic(err)
		}

		return
	}

	logger.Log("event", "pg_setting", "setting", name, "value", value)
	_, err := conn.ExecEx(ctx, fmt.Sprintf(`set %s = '%s';`, name, value), nil)
	if err != nil {
		panic(err)
	}
}

type Index struct {
	name        string
	oid         pgtype.OID
	table       string
	definition  string
	size        string
	constraints []Constraint
}

type Constraint struct {
	name           string
	table          string
	definition     string
	constraintType ConstraintType
}

type ConstraintType byte

const (
	PrimaryKey ConstraintType = 'p'
	ForeignKey ConstraintType = 'f'
	Unique     ConstraintType = 'u'
)

func selectByType(cs []Constraint, ct ConstraintType) []Constraint {
	filtered := []Constraint{}
	for _, c := range cs {
		if c.constraintType == ct {
			filtered = append(filtered, c)
		}
	}

	return filtered
}

func getIndex(ctx context.Context, conn *pgx.Conn, indexName string) (Index, error) {
	var idx = Index{name: indexName}
	rows, err := conn.QueryEx(ctx, selectIndexByName, nil, indexName)
	if err != nil {
		return idx, err
	}

	defer rows.Close()

	for rows.Next() {
		if err := rows.Scan(nil, &idx.oid, &idx.table, &idx.definition, &idx.size); err != nil {
			return idx, err
		}
	}

	if idx.oid == 0 {
		return idx, fmt.Errorf("failed to find index '%s'", indexName)
	}

	idx.constraints, err = getIndexConstraints(ctx, conn, idx.oid)
	return idx, err
}

func getIndexConstraints(ctx context.Context, conn *pgx.Conn, indexOid pgtype.OID) ([]Constraint, error) {
	var constraints = []Constraint{}
	rows, err := conn.QueryEx(ctx, selectIndexConstraintsByOid, nil, indexOid)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var c Constraint
		if err := rows.Scan(&c.name, &c.table, &c.definition, &c.constraintType); err != nil {
			return nil, err
		}

		// c = check constraint, f = foreign key constraint,
		// p = primary key constraint, u = unique constraint,
		// t = constraint trigger, x = exclusion constraint
		switch c.constraintType {
		case PrimaryKey, ForeignKey, Unique:
			constraints = append(constraints, c)
		default:
			return nil, fmt.Errorf("unsupported constraint type on %s: %v", c.name, c.constraintType)
		}
	}

	return constraints, nil
}
