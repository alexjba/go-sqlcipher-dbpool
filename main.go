package main

import (
	"crypto/rand"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/mutecomm/go-sqlcipher" // We require go sqlcipher that overrides default implementation
	// "github.com/status-im/status-go/protocol/sqlite"
)

const (
	// The reduced number of kdf iterations (for performance reasons) which is
	// used as the default value
	// https://github.com/status-im/status-go/pull/1343
	// https://notes.status.im/i8Y_l7ccTiOYq09HVgoFwA
	ReducedKDFIterationsNumber = 3200

	// WALMode for sqlite.
	WALMode      = "wal"
	InMemoryPath = ":memory:"

	dbFile    = "file:/Users/alexjbanca/Repos/status-desktop/Status/data/0x50daf880f5d6f79c12bd75eaeb20984fa646e1d77256fcd0c600465ae6a49995.db"
	dbPass    = "0xc21f6381f45922704ecb6614b23d65be7b63d205355218dc71cabdc3a3001ce3"
)

var (
	max_conns               = 1
	reads                   = 50000
	writes                  = 10
	writeOnDedicatedChannel = 0
	max_rows                = 100000
	conns                   = make(chan *sql.DB, max_conns)
	failedInserts           = uint64(0)
	failedSelects           = uint64(0)
	dbDNSArgs               = ""

	globalDb    *sql.DB
	dataToWrite = make([]byte, 512*1024)
)

func openDB(path string, key string, kdfIterationsNumber int) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", path+dbDNSArgs)
	if err != nil {
		return nil, err
	}

	// Disable concurrent access as not supported by the driver
	db.SetMaxOpenConns(1)

	if _, err = db.Exec("PRAGMA foreign_keys=ON"); err != nil {
		return nil, err
	}
	keyString := fmt.Sprintf("PRAGMA key = '%s'", key)
	if _, err = db.Exec(keyString); err != nil {
		return nil, errors.New("failed to set key pragma")
	}

	if _, err = db.Exec(fmt.Sprintf("PRAGMA kdf_iter = '%d'", kdfIterationsNumber)); err != nil {
		return nil, err
	}

	// readers do not block writers and faster i/o operations
	// https://www.sqlite.org/draft/wal.html
	// must be set after db is encrypted
	var mode string
	err = db.QueryRow("PRAGMA journal_mode=WAL").Scan(&mode)
	if err != nil {
		return nil, err
	}
	if mode != WALMode && path != InMemoryPath {
		return nil, fmt.Errorf("unable to set journal_mode to WAL. actual mode %s", mode)
	}

	return db, nil
}

// OpenDB opens not-encrypted database.
func OpenDB(path string, key string, kdfIterationsNumber int) (*sql.DB, error) {
	return openDB(path, key, kdfIterationsNumber)
}

func checkout() *sql.DB {
	return <-conns
}

func checkin(c *sql.DB) {
	conns <- c
}

func queryRows(wg *sync.WaitGroup, i int) {
	defer wg.Done()
	db := globalDb
	if max_conns > 1 {
		db = checkout()
		defer checkin(db)
	}

	//fmt.Println("Querying", i)
	rows, err := db.Query(fmt.Sprint("SELECT id, mentions FROM user_messages LIMIT ", max_rows))
	if err != nil {
		fmt.Println("Querying", i, err)
		atomic.AddUint64(&failedSelects, 1)
		return
	}
	defer rows.Close()
	count := 0

	for rows.Next() {
		var id string
		var serializedMentions []byte
		err := rows.Scan(&id, &serializedMentions)
		if err != nil {
			fmt.Println("Querying Scan", i, err)
			atomic.AddUint64(&failedSelects, 1)
			return
		}
		count++
	}
	//fmt.Println("Number of rows for iter i: ", i, " ", count)
}

func insertData(wg *sync.WaitGroup, i int) {
	defer wg.Done()
	db := globalDb

	if max_conns > 1 && writeOnDedicatedChannel == 0 {
		db = checkout()
		defer checkin(db)
	}

	tx, err := db.Begin()
	if err != nil {
		fmt.Println("Writing", i, err)
		atomic.AddUint64(&failedInserts, 1)
		return
	}
	stmt, err := tx.Prepare("INSERT INTO test_long_write (data) values(?)")
	if err != nil {
		fmt.Println("Writing Prepare", i, err)
		atomic.AddUint64(&failedInserts, 1)
		return
	}
	defer stmt.Close()
	defer tx.Commit()
	_, err = stmt.Exec(dataToWrite)
	if err != nil {
		fmt.Println("Writing Exec", i, err)
		atomic.AddUint64(&failedInserts, 1)
		return
	}

	//fmt.Println("Done writing", i)
}

func insertDataInLoop(wg *sync.WaitGroup) {
	defer wg.Done()
	for i := 0; i < writes; i++ {
		wg.Add(1)
		go insertData(wg, i)
	}
}

func queryRowsInLoop(wg *sync.WaitGroup) {
	defer wg.Done()
	for i := 0; i < reads; i++ {
		wg.Add(1)
		go queryRows(wg, i)
	}
}

func RunQueries() {
	var wg sync.WaitGroup

	wg.Add(1)
	go insertDataInLoop(&wg)

	wg.Add(1)
	go queryRowsInLoop(&wg)

	wg.Wait()
}

func NewDbConnection() *sql.DB {
	db, err := OpenDB(dbFile, dbPass, 256000)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	return db
}

func DeleteData() {
	fmt.Println("Deleting data..")
	db := globalDb

	if max_conns > 1 && writeOnDedicatedChannel == 0 {
		db = checkout()
		defer checkin(db)
	}

	tx, err := db.Begin()
	if err != nil {
		fmt.Println("Delete", err)
		return
	}
	stmt, err := tx.Prepare("DELETE from test_long_write")
	if err != nil {
		fmt.Println("Delete", err)
		return
	}
	defer stmt.Close()
	defer tx.Commit()
	_, err = stmt.Exec()
	if err != nil {
		fmt.Println("Delete", err)
		return
	}
}

func main() {
	var poolSize string
	flag.StringVar(&poolSize, "poolSize", "1", "pool size: Ex: 1,2,3,4,5")
	flag.IntVar(&writes, "writes", 100, "an int")
	flag.IntVar(&reads, "reads", 100, "an int")
	flag.IntVar(&writeOnDedicatedChannel, "writeOnDedicatedChannel", 0, "Writing is done on a single, dedicated channel")
	flag.StringVar(&dbDNSArgs, "dbDNSArgs", "", "DB DNS args. Ex: ?_foreign_keys=1&_loc=Local&_busy_timeout=5000")
	flag.IntVar(&max_rows, "maxRows", 100, "Max rows to select")

	flag.Parse()

	var runs = strings.Split(poolSize, ",")

	for _, i := range runs {
		connections, err := strconv.Atoi(i)
		if err != nil {
			panic(err)
		}
		max_conns = connections
		run()
	}
}

func run() {

	conns = make(chan *sql.DB, max_conns)

	fmt.Println("Starting pool size: ", max_conns, ", writes: ", writes, ", reads: ", reads, ", Max rows to select: ", max_rows, "Writing on dedicated channel: ", writeOnDedicatedChannel, ", dbDNSArgs: ", dbDNSArgs)

	start := time.Now()
	if max_conns == 1 || writeOnDedicatedChannel == 1 {
		globalDb = NewDbConnection()
		defer globalDb.Close()
	}

	for i := 0; i < max_conns-writeOnDedicatedChannel; i++ {
		db := NewDbConnection()
		defer db.Close()
		conns <- db
	}

	elapsedOpenConn := time.Since(start)
	fmt.Println("Open connections took ", elapsedOpenConn.Seconds(), " seconds")
	rand.Read(dataToWrite)
	start = time.Now()
	RunQueries()
	elapsed := time.Since(start)

	fmt.Println("Failed writes: ", atomic.LoadUint64(&failedInserts), ", Failed reads: ", atomic.LoadUint64(&failedSelects))
	fmt.Println("RunQueries took ", elapsed.Seconds(), " seconds \nQueries per second: ", float64(reads+writes)/elapsed.Seconds())

	DeleteData()
}
