package cockroachdb

import (
  "database/sql"
  "fmt"
  "strconv"
  nurl "net/url"
  "regexp"
  "strings"
  "hash/crc32"
  "context"
  "errors"

  "github.com/db-journey/migrate/direction"
  "github.com/db-journey/migrate/driver"
  "github.com/db-journey/migrate/file"
  "github.com/lib/pq"
  "github.com/cockroachdb/cockroach-go/crdb"
)

const advisoryLockIdSalt uint = 1486364155

var DefaultMigrationsTable = "schema_migrations"
var DefaultLockTable = "schema_lock"

var (
	ErrNilConfig      = fmt.Errorf("no config")
	ErrNoDatabaseName = fmt.Errorf("no database name")
)

var _ driver.Driver = (*Driver)(nil)

type Config struct {
  MigrationsTable string
  LockTable       string
  ForceLock       bool
  DatabaseName    string
}

type Driver struct {
  db     *sql.DB
  config *Config
  lockId string
}

const txDisabledOption = "disable_ddl_transaction" // this is a neat feature

var _ driver.Driver = (*Driver)(nil)

// == INTERFACE METHODS: START =================================================

// Initialize opens and verifies the database handle.
func (driver *Driver) Initialize(url string) error {
  // fmt.Println("Initialize:", url)
  purl, err := nurl.Parse(url)
  if err != nil {
    return err
  }

  re := regexp.MustCompile("^(cockroach(db)?|crdb-postgres)")
  connectString := re.ReplaceAllString(FilterCustomQuery(purl).String(), "postgres")

  db, err := sql.Open("postgres", connectString)
  if err != nil {
    return err
  }

  migrationsTable := purl.Query().Get("x-migrations-table")
  if len(migrationsTable) == 0 {
    migrationsTable = DefaultMigrationsTable
  }

  lockTable := purl.Query().Get("x-lock-table")
  if len(lockTable) == 0 {
    lockTable = DefaultLockTable
  }

  forceLockQuery := purl.Query().Get("x-force-lock")
  forceLock, err := strconv.ParseBool(forceLockQuery)
  if err != nil {
    forceLock = false
  }

  err = PopulateConfig(driver, db, &Config{
    DatabaseName:    purl.Path,
    MigrationsTable: migrationsTable,
    LockTable: lockTable,
    ForceLock: forceLock,
  })
  if err != nil {
    return err
  }

  return lock(driver)
}

// SetDB replaces the current database handle.
func (driver *Driver) SetDB(db *sql.DB) {
  // fmt.Println("SetDB")
  // driver.db = db
}

// Close closes the database handle.
func (driver *Driver) Close() error {
  // fmt.Println("Close")
  if driver.lockId == "" {
    return driver.db.Close()
  }
  defer driver.db.Close()
  return unlock(driver)
}

// FilenameExtension returns "sql".
func (driver *Driver) FilenameExtension() string {
  return "sql"
}

func lock(driver *Driver) error {
  // fmt.Println("Initialize: acquire lock, start")
  return crdb.ExecuteTx(context.Background(), driver.db, nil, func(tx *sql.Tx) error {
    aid, err := GenerateAdvisoryLockId(driver.config.DatabaseName)
    if err != nil {
      return err
    }

    query := `SELECT * FROM "` + driver.config.LockTable + `" WHERE lock_id = $1`
    rows, err := tx.Query(query, aid)
    if err != nil {
      return Error{OrigErr: err, Err: "Failed to fetch migration lock", Query: []byte(query)}
    }
    defer rows.Close()

    // If row exists at all, lock is present
    locked := rows.Next()
    if locked && !driver.config.ForceLock {
      return Error{Err: "Lock could not be acquired; already locked", Query: []byte(query)}
    }

    query = `INSERT INTO "` + driver.config.LockTable + `" (lock_id) VALUES ($1)`
    if _, err := tx.Exec(query, aid) ; err != nil {
      return Error{OrigErr: err, Err: "Failed to set migration lock", Query: []byte(query)}
    }

    driver.lockId = aid

    // fmt.Println("Initialize: acquire lock, finish")
    return nil
  })
}

func unlock(driver *Driver) error {
  if driver.lockId == "" {
    return nil
  }
  // fmt.Println("Close: release lock, start:", driver.lockId)
  query := `DELETE FROM "` + driver.config.LockTable + `" WHERE lock_id = $1`
  if _, err := driver.db.Exec(query, driver.lockId); err != nil {
    if e, ok := err.(*pq.Error); ok {
      // 42P01 is "UndefinedTableError" in CockroachDB
      // https://github.com/cockroachdb/cockroach/blob/master/pkg/sql/pgwire/pgerror/codes.go
      if e.Code == "42P01" {
        // On drops, the lock table is fully removed;  This is fine, and is a valid "unlocked" state for the schema
        // fmt.Println("Migrate: release lock, finish with expected error")
        return nil
      }
    }
    return Error{OrigErr: err, Err: "failed to release migration lock", Query: []byte(query)}
  }
  // fmt.Println("Close: release lock, finish")
  return nil
}

// Migrate performs the migration of any one file.
func (driver *Driver) Migrate(f file.File, pipe chan interface{}) {
  defer close(pipe)
  var err error
  // var aid string
  pipe <- f

  if driver.lockId == "" {
    pipe <- errors.New("Lock has not been acquired, cannot perform migration: " + f.FileName)
    return
  }

  // (1) read file, (2) apply script, (3) insert version
  // fmt.Println("Migrate: read file, start: " + f.FileName)
  if err = f.ReadContent(); err != nil {
    pipe <- err
    return
  }
  // fmt.Println("Migrate: read file, finish: " + f.FileName)

  // cockroach v1 does not allow schema changes and other writes within the same transaction
  // so apply the migration first, in a tx unless explicitly opt-out
  // then, if successful, update the version in a separate tx
  if txDisabled(fileOptions(f.Content)) {
    // fmt.Println("Migrate: running script without tx, start")
    _, err = driver.db.Exec(string(f.Content))
    if err != nil {
      pqErr := err.(*pq.Error)
      offset, err := strconv.Atoi(pqErr.Position)
      if err == nil && offset >= 0 {
        lineNo, columnNo := file.LineColumnFromOffset(f.Content, offset-1)
        errorPart := file.LinesBeforeAndAfter(f.Content, lineNo, 5, 5, true)
        pipe <- fmt.Errorf("%s %v: %s in line %v, column %v:\n\n%s", pqErr.Severity, pqErr.Code, pqErr.Message, lineNo, columnNo, string(errorPart))
      } else {
        pipe <- fmt.Errorf("%s %v: %s", pqErr.Severity, pqErr.Code, pqErr.Message)
      }
      return
    }
    // fmt.Println("Migrate: running script without tx, finish")
  } else {
    // fmt.Println("Migrate: running script with tx, start")
    err = crdb.ExecuteTx(context.Background(), driver.db, nil, func(tx *sql.Tx) error {
      _, err = tx.Exec(string(f.Content))
      return err
    })
    if err != nil {
      pqErr := err.(*pq.Error)
      offset, err := strconv.Atoi(pqErr.Position)
      if err == nil && offset >= 0 {
        lineNo, columnNo := file.LineColumnFromOffset(f.Content, offset-1)
        errorPart := file.LinesBeforeAndAfter(f.Content, lineNo, 5, 5, true)
        pipe <- fmt.Errorf("%s %v: %s in line %v, column %v:\n\n%s", pqErr.Severity, pqErr.Code, pqErr.Message, lineNo, columnNo, string(errorPart))
      } else {
        pipe <- fmt.Errorf("%s %v: %s", pqErr.Severity, pqErr.Code, pqErr.Message)
      }
      return
    }
    // fmt.Println("Migrate: running script with tx, finish")
  }

  // fmt.Println("Migrate: update version, start")
  err = crdb.ExecuteTx(context.Background(), driver.db, nil, func(tx *sql.Tx) error {
    if f.Direction == direction.Up {
      if _, err = tx.Exec(`INSERT INTO "` + driver.config.MigrationsTable + `" (version, name) VALUES ($1, $2)`, f.Version, f.Name); err != nil {
        return err
      }
    } else if f.Direction == direction.Down {
      if _, err = tx.Exec(`DELETE FROM "` + driver.config.MigrationsTable + `" WHERE version=$1`, f.Version); err != nil {
        return err
      }
    }
    return nil
  })

  if err != nil {
    pipe <- err
    // return
  }
  // fmt.Println("Migrate: update version, finish")
}

// Version returns the current migration version.
func (driver *Driver) Version() (file.Version, error) {
  // fmt.Println("Version")
  var version file.Version
  err := driver.db.QueryRow(`SELECT version FROM "` + driver.config.MigrationsTable + `" ORDER BY version DESC LIMIT 1`).Scan(&version)
  if err == sql.ErrNoRows {
    return version, nil
  }
  return version, err
}

// Versions returns the list of applied migrations.
func (driver *Driver) Versions() (file.Versions, error) {
  // fmt.Println("Versions")
  rows, err := driver.db.Query(`SELECT version FROM "` + driver.config.MigrationsTable + `" ORDER BY version DESC`)
  if err != nil {
    return nil, err
  }
  defer rows.Close()

  versions := file.Versions{}
  for rows.Next() {
    var version file.Version
    if err = rows.Scan(&version); err != nil {
      return nil, err
    }
    versions = append(versions, version)
  }

  if err = rows.Err(); err != nil {
    return nil, err
  }

  return versions, err
}

// Execute a SQL statement
func (driver *Driver) Execute(statement string) error {
  // fmt.Println("Execute:", statement)
  // _, err := driver.db.Exec(statement)
  // return err
  return errors.New("Execute method not supported")
}

// == INTERFACE METHODS: END ===================================================

// fileOptions returns the list of options extracted from the first line of the file content.
// Format: "-- <option1> <option2> <...>"
func fileOptions(content []byte) []string {
  firstLine := strings.SplitN(string(content), "\n", 2)[0]
  if !strings.HasPrefix(firstLine, "-- ") {
    return []string{}
  }
  opts := strings.TrimPrefix(firstLine, "-- ")
  return strings.Split(opts, " ")
}

func txDisabled(opts []string) bool {
  for _, v := range opts {
    if v == txDisabledOption {
      return true
    }
  }
  return false
}

func (driver *Driver) ensureVersionTable() error {
  // check if migration table exists
  var count int
  query := `SELECT COUNT(1) FROM information_schema.tables WHERE table_name = $1 AND table_schema = (SELECT current_schema()) LIMIT 1`
  if err := driver.db.QueryRow(query, driver.config.MigrationsTable).Scan(&count); err != nil {
    return &Error{OrigErr: err, Query: []byte(query)}
  }
  if count == 1 {
    return nil
  }

  // if not, create the empty migration table
  query = `CREATE TABLE IF NOT EXISTS "` + driver.config.MigrationsTable + `" (version BIGINT NOT NULL PRIMARY KEY, name STRING(255) NOT NULL, applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP())`
  if _, err := driver.db.Exec(query); err != nil {
    return &Error{OrigErr: err, Query: []byte(query)}
  }
  return nil
}

func (driver *Driver) ensureLockTable() error {
	// check if lock table exists
	var count int
	query := `SELECT COUNT(1) FROM information_schema.tables WHERE table_name = $1 AND table_schema = (SELECT current_schema()) LIMIT 1`
	if err := driver.db.QueryRow(query, driver.config.LockTable).Scan(&count); err != nil {
		return &Error{OrigErr: err, Query: []byte(query)}
	}
	if count == 1 {
		return nil
	}

	// if not, create the empty lock table
	query = `CREATE TABLE IF NOT EXISTS "` + driver.config.LockTable + `" (lock_id INT NOT NULL PRIMARY KEY)`
	if _, err := driver.db.Exec(query); err != nil {
		return &Error{OrigErr: err, Query: []byte(query)}
	}

	return nil
}

// FilterCustomQuery filters all query values starting with `x-`
func FilterCustomQuery(u *nurl.URL) *nurl.URL {
	ux := *u
	vx := make(nurl.Values)
	for k, v := range ux.Query() {
		if len(k) <= 1 || (len(k) > 1 && k[0:2] != "x-") {
			vx[k] = v
		}
	}
	ux.RawQuery = vx.Encode()
	return &ux
}

func PopulateConfig(driver *Driver, instance *sql.DB, config *Config) error {
	if config == nil {
		return ErrNilConfig
	}

	if err := instance.Ping(); err != nil {
		return err
	}

	query := `SELECT current_database()`
	var databaseName string
	if err := instance.QueryRow(query).Scan(&databaseName); err != nil {
		return &Error{OrigErr: err, Query: []byte(query)}
	}

	if len(databaseName) == 0 {
		return ErrNoDatabaseName
	}

	config.DatabaseName = databaseName

	if len(config.MigrationsTable) == 0 {
		config.MigrationsTable = DefaultMigrationsTable
	}

	if len(config.LockTable) == 0 {
		config.LockTable = DefaultLockTable
	}

  driver.db = instance
  driver.config = config

	if err := driver.ensureVersionTable(); err != nil {
		return err
	}

	if err := driver.ensureLockTable(); err != nil {
		return err
	}

	return nil
}

// Error should be used for errors involving queries ran against the database
type Error struct {
  // Optional: the line number
  Line uint

  // Query is a query excerpt
  Query []byte

  // Err is a useful/helping error message for humans
  Err string

  // OrigErr is the underlying error
  OrigErr error
}

func (e Error) Error() string {
  if len(e.Err) == 0 {
    return fmt.Sprintf("%v in line %v: %s", e.OrigErr, e.Line, e.Query)
  }
  return fmt.Sprintf("%v in line %v: %s (details: %v)", e.Err, e.Line, e.Query, e.OrigErr)
}

func GenerateAdvisoryLockId(databaseName string) (string, error) {
	sum := crc32.ChecksumIEEE([]byte(databaseName))
	sum = sum * uint32(advisoryLockIdSalt)
	return fmt.Sprintf("%v", sum), nil
}

func init() {
  drv := Driver{}
  driver.RegisterDriver("cockroach", &drv)
  driver.RegisterDriver("cockroachdb", &drv)
  driver.RegisterDriver("crdb-postgres", &drv)
}
