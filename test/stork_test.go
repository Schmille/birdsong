package test

import (
	"database/sql"
	"fmt"
	"github.com/Schmille/stork"
	_ "github.com/glebarez/go-sqlite"
	"runtime"
	"testing"
)

func getTestMigrations() []stork.Migration {
	out := make([]stork.Migration, 4)

	out[0] = stork.Migration{
		SchemaVersion: 1,
		Up: func(db *sql.DB) {
			// Create basic customer table
			_, err := db.Exec(`CREATE TABLE Customer(
    			Id INTEGER NOT NULL PRIMARY KEY,
    			Firstname TEXT,
    			Lastname TEXT
			);`)
			if err != nil {
				panic(err)
			}
		},
		Down: func(db *sql.DB) {
			_, err := db.Exec("DROP TABLE Customer")
			if err != nil {
				panic(err)
			}
		},
	}
	out[1] = stork.Migration{
		SchemaVersion: 2,
		Up: func(db *sql.DB) {
			_, err := db.Exec("ALTER TABLE Customer ADD COLUMN Birthday TEXT")
			if err != nil {
				panic(err)
			}
		},
		Down: func(db *sql.DB) {
			_, err := db.Exec("ALTER TABLE Customer DROP COLUMN Birthday")
			if err != nil {
				panic(err)
			}
		},
	}

	out[2] = stork.Migration{
		SchemaVersion: 3,
		Up: func(db *sql.DB) {
			_, err := db.Exec("ALTER TABLE Customer ADD COLUMN Pet TEXT")
			if err != nil {
				panic(err)
			}
		},
		Down: func(db *sql.DB) {
			_, err := db.Exec("ALTER TABLE Customer DROP COLUMN Pet")
			if err != nil {
				panic(err)
			}
		},
	}

	out[3] = stork.Migration{
		SchemaVersion: 4,
		Up:            stork.NoopDBFunc(),
		Down:          stork.NoopDBFunc(),
	}

	return out
}

func testGetVersion(db *sql.DB) int {
	rows, err := db.Query("PRAGMA user_version;")
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	var version int
	rows.Next()
	rows.Scan(&version)

	return version
}

var TestDB *sql.DB

func setup() *sql.DB {
	if TestDB != nil {
		// Kill the memory DB
		TestDB.Close()
		TestDB = nil
		runtime.GC()
	}

	testdb, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		panic(err)
	}
	testdb.SetMaxOpenConns(1)
	TestDB = testdb

	return TestDB
}

func testSetVersion(db *sql.DB, version int) {
	_, err := db.Exec(fmt.Sprintf("PRAGMA user_version = %d;", version))
	if err != nil {
		panic(err)
	}
}

func TestMigrator_MigrateTo(t *testing.T) {
	m := stork.NewMigrator(testGetVersion, testSetVersion)

	for _, mig := range getTestMigrations() {
		m.RegisterMigrations(mig)
	}

	db := setup()
	m.MigrateTo(db, 3)

	_, err := db.Exec(`INSERT INTO Customer(Id, Pet) VALUES(1, "Garry")`)
	if err != nil {
		t.Error(err.Error())
		t.Fail()
	}
}

func TestMigrator_MigrateTo_ShouldPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic")
		}
	}()

	m := stork.NewMigrator(testGetVersion, testSetVersion)
	m.MigrateTo(setup(), -1)
}

func TestMigrator_MigrateTo_Should_Break_Sequence(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic")
		}
	}()

	m := stork.NewMigrator(testGetVersion, testSetVersion)

	m.RegisterMigrations(stork.Migration{
		SchemaVersion: 1,
		Up:            stork.NoopDBFunc(),
		Down:          stork.NoopDBFunc(),
	},
		stork.Migration{
			SchemaVersion: 5,
			Up:            stork.NoopDBFunc(),
			Down:          stork.NoopDBFunc(),
		})

	m.MigrateTo(setup(), 12)
}
