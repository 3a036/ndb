package ndb

type Row interface {
	GetUID() int                //主键
	Index() map[string][]string //索引
	Stat() map[string][]string  //统计列
	Encode() []byte
}
