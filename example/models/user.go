package models

import "encoding/json"

import (
	"github.com/shopspring/decimal"
)

type User struct {
	UID    int
	GID    int
	TCC    decimal.Decimal
	ETH    decimal.Decimal
	NASH   decimal.Decimal
	Desc   string
	Worker map[int]bool
	I1     int
}

func (u *User) GetUID() int {
	return u.UID
}

func (u *User) Index() [][]string {
	return nil
}

func (u *User) Encode() []byte {
	bs, _ := json.Marshal(u)
	return bs
}

func (u *User) Category() string {
	return "User"
}
