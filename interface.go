package ndb

type Row interface {
	GetUID() int                //主键
	Index() map[string][]string //索引
	Encode() []byte
}
