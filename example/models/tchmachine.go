package models

import "encoding/json"

type TchMachine struct {
	ID  int
	GID int
	UID int
}

var (
	indexes = map[string][]string{
		"guid": {"GID", "UID"},
		"gid":  {"GID"},
		"uid":  {"UID"},
	}
	stats = map[string][]string{
		"guid": {"GID", "UID"},
	}
)

//索引列的值暂不支持修改，如需修改，可以先删除记录，再插入新记录
func (t *TchMachine) Index() map[string][]string {
	return indexes
}

//索引列的值暂不支持修改，如需修改，可以先删除记录，再插入新记录
func (t *TchMachine) Stat() map[string][]string {
	return stats
}

func (t *TchMachine) GetUID() int {
	return t.ID
}

func (u *TchMachine) Encode() []byte {
	bs, _ := json.Marshal(u)
	return bs
}

func (u *TchMachine) Category() string {
	return "TchMachine"
}
