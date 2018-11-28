package controller

import (
	"fmt"
	"github.com/helloshiki/ndb"
	"github.com/helloshiki/ndb/example/models"
	"github.com/shopspring/decimal"
	"log"
)

//用户转账
func Transfer(fromID int, toID int, asset string, amount decimal.Decimal) error {
	from := &models.User{UID: fromID}
	to := &models.User{UID: toID}

	//先检查转账的两个账号是否存在
	if ndb.Get(from) == nil || ndb.Get(to) == nil {
		log.Printf("user %d or %d not found", fromID, toID)
		return fmt.Errorf("user %d or %d not found", fromID, toID)
	}

	//先扣钱
	if b, e, err := ndb.UpdateField(from, asset, "DEC", amount, true); err != nil {
		log.Printf("user %d asset[%s] DESC failed", fromID, asset)
		return err
	} else {
		log.Printf("id %d's %s change from %s to %s", fromID, asset, b, e)
	}

	//再发钱
	if b, e, err := ndb.UpdateField(to, asset, "INC", amount, true); err != nil {
		log.Printf("user %d asset[%s] INC failed", toID, asset)
		return err
	} else {
		log.Printf("id %d's %s change from %s to %s", toID, asset, b, e)

	}

	return nil
}
