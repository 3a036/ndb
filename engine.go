package ndb

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

func GetByIndex(row Row, indexName string) []int {
	return DefaultDB.GetByIndex(row, indexName)
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

func Insert(row Row) error {
	return DefaultDB.Insert(row)
}

func Load(row Row) error {
	return DefaultDB.Load(row)
}

func Update(row Row) error {
	return DefaultDB.Update(row)
}

func UpdateFunc(row Row, cb func(row Row) bool) error {
	return DefaultDB.UpdateFunc(row, cb)
}

func GetTable(tableName string) *Table {
	return DefaultDB.GetTable(tableName)
}

func SetPutTx(fn func(*Transaction)) {
	putTrx = fn
}
