package ndb

import (
	"fmt"
	"github.com/shopspring/decimal"
	"log"
	"reflect"
	"sort"
	"sync"
	"time"
)

func getTableName(row Row) string {
	val := reflect.ValueOf(row)
	typ := reflect.Indirect(val).Type()
	tableName := typ.Name()

	if val.Kind() != reflect.Ptr {
		panic(fmt.Errorf("cannot use non-ptr struct %s", tableName))
	}

	return tableName
}

func newTable(dbName, tableName string) *Table {
	return &Table{
		dbName:     dbName,
		tableName:  tableName,
		rows:       make([]Row, 0),
		idxIndexes: make(map[int]int),
		metas:      make(map[int]*MetaInfo),
		indexes:    make(map[string][]int),
		stats:      make(map[string][]*Stat),
		fieldStats: make(map[string]map[string]int),
		sorting:    make(map[string]bool),
		statting:   false,
		sortlock:   &sync.Mutex{},
		statlock:   &sync.Mutex{},
		lock:       &sync.Mutex{},
		allocChan:  make(chan int, ROWSIZE),
	}
}

func (tb *Table) nextIdx() int {
	select {
	case index := <-tb.allocChan:
		return index
	default:
		allocSize := len(tb.rows)
		toAppend := make([]Row, ROWSIZE/2)
		tb.rows = append(tb.rows, toAppend...)
		for i := 0; i < ROWSIZE/2; i++ {
			tb.allocChan <- allocSize + i
		}
		return <-tb.allocChan
	}
}

func (tb *Table) putIdx(idx int) {
	select {
	case tb.allocChan <- idx:
		return
	default:
		log.Printf("table %s's chan is full", tb.tableName)
	}
}

func (tb *Table) getIndexKey(row Row, indexName string) string {
	if indexName == PRIMARYKEY {
		return indexName
	}
	if indexs := row.Index(); indexs != nil {
		if indexFields, ok := indexs[indexName]; ok {
			indexKey := indexName
			val := reflect.ValueOf(row)
			sort.StringSlice(indexFields).Sort()
			for i := 0; i < len(indexFields); i++ {
				indexKey += fmt.Sprintf(":%v", reflect.Indirect(val).FieldByName(indexFields[i]))
			}
			return indexKey
		}
	}
	return ""
}

func (table *Table) sortIndex(index string) {
	slock := table.sortlock
	slock.Lock()

	if table.sorting[index] {
		slock.Unlock()
		return
	}

	table.sorting[index] = true
	slock.Unlock()

	time.AfterFunc(3*time.Second, func() {
		slock := table.sortlock
		slock.Lock()
		table.sorting[index] = false
		slock.Unlock()

		start := time.Now().Unix()

		lock := table.lock
		lock.Lock()

		indexes := table.indexes
		sort.IntSlice(indexes[index]).Sort()

		length := len(indexes[index])
		lock.Unlock()

		end := time.Now().Unix()
		log.Printf("sort index %s:%s %d records finished in %d second", table.tableName, index, length, end-start)
	})
}

func (table *Table) sortStat(statName string) {
	slock := table.statlock
	slock.Lock()

	if table.statting {
		slock.Unlock()
		return
	}
	table.statting = true
	slock.Unlock()

	time.AfterFunc(3*time.Second, func() {
		slock := table.statlock
		slock.Lock()
		table.statting = false
		slock.Unlock()

		start := time.Now().Unix()
		lock := table.lock
		lock.Lock()

		sort.SliceStable(table.stats[statName], func(i, j int) bool { return table.stats[statName][i].Count > table.stats[statName][j].Count })

		lock.Unlock()
		end := time.Now().Unix()
		log.Printf("sort stat %s:%s %d records finished in %d second", table.tableName, statName, len(table.stats[statName]), end-start)

	})

}

func (table *Table) insert(row Row, isLoad bool) error {
	tableName := table.tableName
	uid := row.GetUID()

	lock := table.lock
	lock.Lock()
	defer lock.Unlock()

	if rid, ok := table.idxIndexes[uid]; ok { //exist
		log.Printf("record id[%d] is exist in table %s %d row", uid, tableName, rid)
		return DBErrDup // fmt.Errorf("record %d is exist in %s", uid, tableName)
	}

	idx := table.nextIdx()

	//创建meta
	meta := &MetaInfo{Version: 1, UpdateStamp: time.Now(), SavedVersion: 0, SavedStamp: time.Now()}
	////从数据库加载时load需传值，避免回写
	if isLoad {
		meta.SavedVersion = 1
	}
	table.metas[uid] = meta

	table.rows[idx] = row
	table.idxIndexes[uid] = idx

	//发起持久化指令
	table.putTx("INSERT", uid, meta.Version)
	//putTrx(&Transaction{Cmd: "INSERT", DBName:table.dbName, TableName: tableName, ID: uid, Version: meta.Version})

	//添加到主键列表
	pk := PRIMARYKEY
	table.indexes[pk] = append(table.indexes[pk], uid)
	//列表排序
	table.sortIndex(pk)

	//log.Printf("insert record id[%d] in table %s's %d row", id, tableName, rid)

	indexs := row.Index()
	if indexs == nil {
		return nil
	}

	//存在索引，创建索引
	for indexName, indexFields := range indexs {
		if len(indexFields) == 0 {
			continue
		}
		indexKey := table.getIndexKey(row, indexName)
		table.indexes[indexKey] = append(table.indexes[indexKey], uid)
		//索引排序
		table.sortIndex(indexKey)
	}

	stats := row.Stat()
	if stats == nil {
		return nil
	}

	//存在统计列，创建统计数据
	for statName, statFields := range stats {
		if len(statFields) == 0 {
			continue
		}
		sort.StringSlice(statFields).Sort() //先排
		val := reflect.ValueOf(row)
		statKey := make(map[string]interface{})
		statKeyStr := ""
		for i := 0; i < len(statFields); i++ {
			statKey[statFields[i]] = reflect.Indirect(val).FieldByName(statFields[i]).Interface()
			statKeyStr += fmt.Sprintf(":%v", reflect.Indirect(val).FieldByName(statFields[i]))
		}

		log.Printf("stat key of %s is %+v", statName, statKey)

		if _, ok := table.fieldStats[statName]; !ok {
			table.fieldStats[statName] = make(map[string]int)
		}

		if _, ok := table.fieldStats[statName][statKeyStr]; !ok { //创建新记录
			table.fieldStats[statName][statKeyStr] = 1
			table.stats[statName] = append(table.stats[statName], &Stat{statKey, 1})
		} else {
			table.fieldStats[statName][statKeyStr] += 1
			for i := 0; i < len(table.stats[statName]); i++ {
				if reflect.DeepEqual(table.stats[statName][i].StatKey, statKey) {
					table.stats[statName][i].Count += 1
					break
				}
			}
		}
		table.sortStat(statName)
	}

	return nil
}

//全覆盖更新
func (table *Table) Update(row Row) error {
	tableName := table.tableName
	uid := row.GetUID()

	lock := table.lock
	lock.Lock()
	defer lock.Unlock()

	if rid, ok := table.idxIndexes[uid]; ok {
		table.rows[rid] = row
		//更新meta
		meta := table.metas[uid]
		meta.Version += 1
		meta.UpdateStamp = time.Now()

		//发起持久化指令
		table.putTx("UPDATE", uid, meta.Version)
	} else {
		log.Printf("record %d is not exist in table %s", uid, tableName)
		return fmt.Errorf("record %d is not exist in table %s", uid, tableName)
	}
	return nil
}

func (table *Table) UpdateFunc(row Row, cb func(row Row) bool) error {
	tableName := table.tableName
	uid := row.GetUID()

	lock := table.lock
	lock.Lock()
	defer lock.Unlock()

	rid, ok := table.idxIndexes[uid]
	if !ok {
		log.Printf("record %d is not exist in table %s", uid, tableName)
		return fmt.Errorf("record %d is not exist in table %s", uid, tableName)
	}

	if cb(table.rows[rid]) == false {
		log.Printf("record %d in table %s callback failed", uid, tableName)
		return fmt.Errorf("record %d in table %s callback failed", uid, tableName)
	}
	//更新meta
	meta := table.metas[uid]
	meta.Version += 1
	meta.UpdateStamp = time.Now()

	//发起持久化指令
	table.putTx("UPDATE", uid, meta.Version)
	return nil
}

//cmd支持REPLACE，INC，DEC，ZERO，某些特殊类型只支持REPLACE，strict是否严格模式，当严格模式时，当前行必须已被序列化, 成功时，返回该列更新前后的值
func (table *Table) UpdateField(row Row, fieldName string, cmd string, value interface{}, strict bool) (string, string, error) {
	tableName := table.tableName
	uid := row.GetUID()

	lock := table.lock
	lock.Lock()
	defer lock.Unlock()

	b := ""
	e := ""

	if strict { //严格模式，主要用在用户资产转账场景
		if meta, ok := table.metas[uid]; !ok || (meta.Version != meta.SavedVersion &&
			meta.UpdateStamp.After(meta.SavedStamp.Add(5*time.Second))) {
			log.Printf("row %d in table[%s] strict check failed", uid, tableName)
			return b, e, fmt.Errorf("row %d in table[%s] strict check failed", uid, tableName)
		}
	}

	if rid, ok := table.idxIndexes[uid]; ok {
		val := reflect.ValueOf(table.rows[rid]).Elem()

		switch val.FieldByName(fieldName).Type().Kind() {
		case reflect.Map, reflect.String, reflect.Struct: //直接替换的类型
			if val.FieldByName(fieldName).Type().Name() == "Decimal" {
				d1 := val.FieldByName(fieldName).Interface().(decimal.Decimal)
				d2 := value.(decimal.Decimal)
				b = d1.String()
				switch cmd {
				case "REPLACE":
					val.FieldByName(fieldName).Set(reflect.ValueOf(value))
				case "INC":
					val.FieldByName(fieldName).Set(reflect.ValueOf(d1.Add(d2)))
				case "DEC":
					if d1.GreaterThanOrEqual(d2) {
						val.FieldByName(fieldName).Set(reflect.ValueOf(d1.Sub(d2)))
					} else {
						return b, e, DBErrDec //  fmt.Errorf("record %d %s not enough", uid, fieldName)
					}
				case "ZERO":
					val.FieldByName(fieldName).Set(reflect.ValueOf(decimal.Zero))
				default:
					panic(fmt.Errorf("unsupport update cmd %s ", cmd))
				}
				e = val.FieldByName(fieldName).Interface().(decimal.Decimal).String()
			} else { //REPLACE
				b = fmt.Sprintf("%+v", val.FieldByName(fieldName))
				val.FieldByName(fieldName).Set(reflect.ValueOf(value))
				e = fmt.Sprintf("%+v", value)
			}
		case reflect.Int:
			b = fmt.Sprintf("%+v", val.FieldByName(fieldName))
			switch cmd {
			case "REPLACE":
				val.FieldByName(fieldName).SetInt(int64(value.(int)))
			case "INC":
				val.FieldByName(fieldName).SetInt(val.FieldByName(fieldName).Int() + int64(value.(int)))
			case "DEC":
				if val.FieldByName(fieldName).Int() >= int64(value.(int)) {
					val.FieldByName(fieldName).SetInt(val.FieldByName(fieldName).Int() - int64(value.(int)))
				} else {
					return "", "", DBErrDec // fmt.Errorf("record %d %s not enough", uid, fieldName)
				}
			case "ZERO":
				val.FieldByName(fieldName).SetInt(0)
			default:
				panic(fmt.Errorf("unsupport update cmd %s ", cmd))
			}
			e = fmt.Sprintf("%+v", val.FieldByName(fieldName))
		default:
			log.Printf("unsupport type is %+v in table[%s],field[%s]", val.FieldByName(fieldName).Type().Kind(), tableName, fieldName)
			return "", "", DBErrNotSupport // fmt.Errorf("unsupport type is %+v in table[%s],field[%s]", val.FieldByName(fieldName).Type().Kind(), tableName, fieldName)
		}
		//更新meta
		meta := table.metas[uid]
		meta.Version += 1
		meta.UpdateStamp = time.Now()

		//发起持久化指令
		table.putTx("UPDATE", uid, meta.Version)
	} else {
		log.Printf("record %d is not exist in table %s", uid, tableName)
		return b, e, DBErrNotFound // fmt.Errorf("record %d is not exist in table %s", uid, tableName)
	}
	return b, e, nil
}

func (table *Table) putTx(cmd string, uid int, version uint64) {
	putTrx(&Transaction{
		Cmd:       cmd,
		DBName:    table.dbName,
		TableName: table.tableName,
		UID:       uid,
		Version:   version,
	})
}

func (table *Table) Get(row Row) Row {
	tableName := table.tableName

	uid := row.GetUID()
	lock := table.lock
	lock.Lock()
	defer lock.Unlock()

	if rid, ok := table.idxIndexes[uid]; ok {
		return table.rows[rid]
	}

	log.Printf("record %d is not exist in table %s", uid, tableName)
	return nil
}

/*使用索引名查找， 相关索引列都要赋值*/
func (table *Table) GetByIndex(row Row, indexName string) []int {
	indexKey := table.getIndexKey(row, indexName)
	if indexKey == "" {
		return nil
	}

	lock := table.lock
	lock.Lock()
	defer lock.Unlock()

	return table.indexes[indexKey]
}

func (table *Table) GetStat(row Row, statName string, all bool) []*Stat {
	lock := table.lock
	lock.Lock()
	defer lock.Unlock()

	if !all { //查特定条件， todo
		return nil
	}

	//查全部
	return table.stats[statName]
}

func (table *Table) Delete(row Row) {
	tableName := getTableName(row)
	uid := row.GetUID()
	lock := table.lock
	lock.Lock()
	defer lock.Unlock()

	idx, ok := table.idxIndexes[uid]
	if !ok {
		return
	}

	meta := table.metas[uid]
	delete(table.idxIndexes, uid)
	delete(table.metas, uid)
	table.putIdx(idx)

	//发起持久化指令
	table.putTx("DELETE", uid, meta.Version)

	//删除主键列表
	pk := PRIMARYKEY
	indexArr := table.indexes[pk]
	arrLen := len(indexArr)
	for i := 0; i < arrLen; i++ {
		if indexArr[i] == uid {
			indexArr[i] = indexArr[arrLen-1]
			indexArr = indexArr[:arrLen-1]
			break
		}
	}
	table.indexes[pk] = indexArr

	//列表排序
	table.sortIndex(pk)

	log.Printf("delete recoed %d from %s", uid, tableName)

	indexs := row.Index()
	if indexs == nil {
		return
	}

	//存在索引，删除索引
	for indexName, indexFields := range indexs {
		if len(indexFields) == 0 {
			continue
		}
		indexKey := table.getIndexKey(row, indexName)
		indexArr := table.indexes[indexKey]
		arrLen := len(indexArr)
		for i := 0; i < arrLen; i++ {
			if indexArr[i] == uid {
				indexArr[i] = indexArr[arrLen-1]
				indexArr = indexArr[:arrLen-1]
				break
			}
		}
		table.indexes[indexKey] = indexArr

		//索引排序
		table.sortIndex(indexKey)
	}

	stats := row.Stat()
	if stats == nil {
		return
	}

	//存在统计列，删除统计数据
	for statName, statFields := range stats {
		if len(statFields) == 0 {
			continue
		}
		sort.StringSlice(statFields).Sort() //先排
		val := reflect.ValueOf(row)
		statKey := make(map[string]interface{})
		statKeyStr := ""
		for i := 0; i < len(statFields); i++ {
			statKey[statFields[i]] = reflect.Indirect(val).FieldByName(statFields[i]).Interface()
			statKeyStr += fmt.Sprintf(":%v", reflect.Indirect(val).FieldByName(statFields[i]))
		}

		log.Printf("stat key of %s is %+v", statName, statKey)

		if _, ok := table.fieldStats[statName]; !ok {
			panic("what!!!")
		}

		if _, ok := table.fieldStats[statName][statKeyStr]; !ok {
			panic("what!!!!!!")
		} else {
			table.fieldStats[statName][statKeyStr] -= 1
			if table.fieldStats[statName][statKeyStr] < 0 {
				panic("what!!!!!!!!!")
			}
			for i := 0; i < len(table.stats[statName]); i++ {
				if reflect.DeepEqual(table.stats[statName][i].StatKey, statKey) {
					table.stats[statName][i].Count -= 1
					if table.stats[statName][i].Count < 0 {
						panic("what!!!!!!!!!!!!!")
					}
					break
				}
			}
		}
		table.sortStat(statName)
	}
}

func (table *Table) GetRowBytes(uid int, version uint64) (uint64, []byte) {
	lock := table.lock

	lock.Lock()
	defer lock.Unlock()

	meta, ok := table.metas[uid]
	if !ok || meta.SavedVersion >= version { //记录已被删除或当前版本小于已保存版本
		return 0, nil
	}

	idx := table.idxIndexes[uid]
	obj := table.rows[idx]
	ver := meta.Version

	buf := obj.Encode()

	return ver, buf
}

func (table *Table) UpdateSavedVersion(uid int, version uint64) {
	lock := table.lock
	lock.Lock()
	defer lock.Unlock()

	if meta, ok := table.metas[uid]; ok && meta.SavedVersion < version {
		meta.SavedVersion = version
		meta.SavedStamp = time.Now()
	}
}
