package ndb

import "sync"

const (
	K = 1024
	M = 1024 * K
	G = 1024 * M
)

const (
	ROWSIZE    = M
	PRIMARYKEY = "pk"
)

type MetaInfo struct {
	Version      uint64
	UpdateStamp  int64
	SavedVersion uint64
	SavedStamp   int64
}

type Table struct {
	dbName string
	tableName  string
	rows       []Row             // map[idx] => row
	idxIndexes map[int]int       // map[uid] => idx
	metas      map[int]*MetaInfo // map[uid] => idx
	indexes    map[string][]int  // map[indexKey] => [uid, uid, uid]
	sorting    map[string]bool   // map[indexKey] => bool
	sortlock   *sync.Mutex       // map[indexKey] => lock
	lock       *sync.Mutex
	allocChan  chan int
}

type DB struct {
	rwLock sync.RWMutex
	dbName string
	tables map[string]*Table
}


type Transaction struct {
	DBName string
	TableName string
	Cmd       string
	UID        int
	Version   uint64
}

var (
	putTrx func(*Transaction)
)