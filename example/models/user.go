package models

import "encoding/json"

type User struct {
	UID int
	GID int
	TCC int
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
