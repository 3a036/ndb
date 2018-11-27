package controller

import (
	"fmt"
	"github.com/helloshiki/ndb"
	"github.com/helloshiki/ndb/example/models"
	"log"
)

//用户转账
func Transfer(fromID int, toID int, asset string, amount int64) error {
	from := &models.User{UID: fromID}
	to := &models.User{UID: toID}

	//先检查转账的两个账号是否存在
	if ndb.Get(from) == nil || ndb.Get(to) == nil {
		log.Printf("user %d or %d not found", fromID, toID)
		return fmt.Errorf("user %d or %d not found", fromID, toID)
	}

	//先扣钱
	if err := ndb.UpdateFiled(from, asset, "DESC", amount); err != nil {
		log.Printf("user %d asset[%s] DESC failed", fromID, asset)
		return err
	}

	//再发钱
	if err := ndb.UpdateFiled(to, asset, "INC", amount); err != nil {
		log.Printf("user %d asset[%s] INC failed", toID, asset)
		return err
	}

	return nil
}
