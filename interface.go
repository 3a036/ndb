package ndb

type Row interface {
	GetUID() int       //主键
	Index() [][]string //索引
	Marshal() ([]byte, error)
	Category() string
}
