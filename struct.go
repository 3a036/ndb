package ndb

import (
	"errors"
	"sync"
	"time"
)

const (
	K = 1024
	M = 1024 * K
	G = 1024 * M
)

const (
	ROWSIZE    = M
	PRIMARYKEY = "pk"
)

var (
	DBErrDec        = errors.New("dec fail")
	DBErrSync       = errors.New("not sync")
	DBErrNotFound   = errors.New("not found")
	DBErrNotSupport = errors.New("not support")
	DBErrDup        = errors.New("duplicate")
)

type MetaInfo struct {
	Version      uint64
	UpdateStamp  time.Time
	SavedVersion uint64
	SavedStamp   time.Time
}

type Stat struct {
	StatKey map[string]interface{}
	Count   int
}

type Table struct {
	dbName     string
	tableName  string
	rows       []Row                     // map[idx] => row
	idxIndexes map[int]int               // map[uid] => idx
	metas      map[int]*MetaInfo         // map[uid] => idx
	indexes    map[string][]int          // map[indexKey] => [uid, uid, uid]
	stats      map[string][]*Stat        //map[statName] => [statinfo, statinfo]
	fieldStats map[string]map[string]int //map[statName] => map[statKey] => count
	sorting    map[string]bool           // map[indexKey] => bool
	sortlock   *sync.Mutex               // map[indexKey] => lock
	statting   bool
	statlock   *sync.Mutex
	lock       *sync.Mutex
	allocChan  chan int
}

type DB struct {
	rwLock sync.RWMutex
	dbName string
	tables map[string]*Table
}

type Transaction struct {
	DBName    string
	TableName string
	Cmd       string
	UID       int
	Version   uint64
}

var (
	putTrx func(*Transaction)
)
