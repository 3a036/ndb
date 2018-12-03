package ndb

import (
	"fmt"
	"reflect"
)

const DefaultDBName = "default"

var (
	DefaultDB *DB
	dbMap     = map[string]*DB{}
)

func init() {
	DefaultDB = newDB(DefaultDBName)
	dbMap[DefaultDBName] = DefaultDB
}

func GetDB(dbName string) *DB {
	return dbMap[dbName]
}

func CreateTable(row Row) {
	DefaultDB.CreateTable(row)
}

func Get(row Row) Row {
	return DefaultDB.Get(row)
}

func MustGet(cond Row) Row {
	row := DefaultDB.Get(cond)
	if row == nil {
		panic(fmt.Errorf("MustGet fail %+v", cond))
	}
	return row
}

func GetByIndex(row Row, indexName string) []int {
	return DefaultDB.GetByIndex(row, indexName)
}

func MustFirst(cond Row, indexName string) Row {
	ridArr := DefaultDB.GetByIndex(cond, indexName)
	if len(ridArr) > 1 {
		panic(fmt.Errorf("MustFirst %s fail. %d", indexName, len(ridArr)))
	}

	if len(ridArr) == 0 {
		return nil
	}

	rid := ridArr[0]
	val := reflect.ValueOf(cond).Elem()
	val.FieldByName("Rid").Set(reflect.ValueOf(rid))
	return MustGet(cond)
}

func GetStat(row Row, statName string, all bool) []*Stat {
	return DefaultDB.GetStat(row, statName, all)
}

func Delete(row Row) {
	DefaultDB.Delete(row)
}

func UpdateField(row Row, fieldName string, cmd string, value interface{}, strict bool) (string, string, error) {
	return DefaultDB.UpdateFiled(row, fieldName, cmd, value, strict)
}

func MustUpdateField(cond Row, fieldName string, cmd string, value interface{}) {
	_, _, err := UpdateField(cond, fieldName, cmd, value, false)
	if err != nil {
		panic(fmt.Errorf("MustUpdateField fail %+v %s %s %+v %s", cond, fieldName, cmd, value, err.Error()))
	}
}

func Insert(row Row) error {
	return DefaultDB.Insert(row)
}

func MustInsert(cond Row) {
	if err := Insert(cond); err != nil {
		panic(fmt.Errorf("MustInsert fail %+v %+v", cond, err))
	}
}

func Load(row Row) error {
	return DefaultDB.Load(row)
}

func MustLoad(cond Row) {
	if err := Load(cond); err != nil {
		panic(fmt.Errorf("MustLoad fail %+v %+v", cond, err))
	}
}

func Update(row Row) error {
	return DefaultDB.Update(row)
}

func MustUpdate(cond Row) {
	if err := Update(cond); err != nil {
		panic(fmt.Errorf("MustUpdate fail %+v %+v", cond, err))
	}
}

func UpdateFunc(row Row, cb func(row Row) bool) error {
	return DefaultDB.UpdateFunc(row, cb)
}

func MustUpdateFunc(cond Row, cb func(row Row) bool) {
	if err := UpdateFunc(cond, cb); err != nil {
		panic(fmt.Errorf("MustUpdateFunc fail %+v %+v", cond, err))
	}
}

func GetTable(tableName string) *Table {
	return DefaultDB.GetTable(tableName)
}

func SetPutTx(fn func(*Transaction)) {
	putTrx = fn
}
