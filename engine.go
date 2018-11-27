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

func Delete(row Row) {
	DefaultDB.Delete(row)
}

func UpdateField(row Row, fieldName string, cmd string, value interface{}) error {
	return DefaultDB.UpdateFiled(row, fieldName, cmd, value)
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

func GetTable(tableName string) *Table {
	return DefaultDB.GetTable(tableName)
}

func SetPutTx(fn func(*Transaction)) {
	putTrx = fn
}
