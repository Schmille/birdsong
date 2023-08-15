package stork

import (
	"database/sql"
	"errors"
	"fmt"
)

type DatabaseFunction func(db *sql.DB)
type SetVersionFunction func(db *sql.DB, version int)
type GetVersionFunc func(db *sql.DB) int

type Migration struct {
	SchemaVersion int
	Up            DatabaseFunction
	Down          DatabaseFunction
}

type Migrator struct {
	migrations    []Migration
	CheckSequence bool
	GetVersion    GetVersionFunc
	SetVersion    SetVersionFunction
	BeforeAll     DatabaseFunction
	AfterAll      DatabaseFunction
	BeforeEach    DatabaseFunction
	AfterEach     DatabaseFunction
}

func NoopDBFunc() DatabaseFunction {
	return func(db *sql.DB) {
		// Noop
	}
}

func (m *Migrator) RegisterMigrations(migrations ...Migration) {
	for _, migration := range migrations {
		m.migrations = append(m.migrations, migration)
	}
}

func (m *Migrator) MigrateTo(db *sql.DB, version int) {

	if version <= 0 {
		panic(errors.New("SchemaVersion must be >= 1"))
	}

	if m.CheckSequence {
		err := m.validateSequence()
		if err != nil {
			panic(err)
		}
	}

	current := m.GetVersion(db)

	// Should go up
	if m.BeforeAll != nil {
		m.BeforeAll(db)
	}

	if version > current {
		for _, migration := range m.migrations {
			if migration.SchemaVersion <= current {
				continue
			}

			if migration.SchemaVersion <= version {

				if m.BeforeEach != nil {
					m.BeforeEach(db)
				}

				migration.Up(db)
				m.SetVersion(db, migration.SchemaVersion)

				if m.AfterEach != nil {
					m.AfterEach(db)
				}
			}

		}
	} else if version < current {
		for _, migration := range reverse(m.migrations) {
			if migration.SchemaVersion < version {
				break
			}

			if m.BeforeEach != nil {
				m.BeforeEach(db)
			}

			if migration.SchemaVersion >= version {
				migration.Down(db)
				m.SetVersion(db, migration.SchemaVersion)
			}

			if m.AfterEach != nil {
				m.AfterEach(db)
			}
		}
	}

	if m.AfterAll != nil {
		m.AfterAll(db)
	}
}

func (m *Migrator) MigrateToLatest(db *sql.DB) {
	latest := m.migrations[len(m.migrations)-1].SchemaVersion
	m.MigrateTo(db, latest)
}

func NewMigrator(getVersion GetVersionFunc, setVersion SetVersionFunction) Migrator {
	return Migrator{
		GetVersion:    getVersion,
		SetVersion:    setVersion,
		CheckSequence: true,
		migrations:    make([]Migration, 0),
	}
}

func (m *Migrator) validateSequence() error {
	var last int
	for i, migration := range m.migrations {

		// Allow for migrations to start at any number
		// Useful when migrating from existing systems
		if i == 0 {
			last = migration.SchemaVersion
			continue
		}

		current := migration.SchemaVersion
		if current != last+1 {
			return errors.New(fmt.Sprintf("Sequence break! Migration %d is followed by %d", last, current))
		}
		last = current
	}

	return nil
}

func reverse(slice []Migration) []Migration {
	out := make([]Migration, len(slice))
	j := 0

	for i := len(slice) - 1; i >= 0; i-- {
		out[i] = slice[j]
		j++
	}

	return out
}
