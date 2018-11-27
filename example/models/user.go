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

func (u *User) Marshal() ([]byte, error) {
	return json.Marshal(u)
}

func (u *User) Category() string {
	return "User"
}