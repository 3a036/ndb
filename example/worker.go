package main

import (
	//"encoding/json"
	"log"
	"time"
)

const (
	WORKCNT = 1
)

func work() {
	for i := 0; i < WORKCNT; i++ {
		go func() {
			ticker := time.NewTicker(3 * time.Second)
			counter := 0
			printedCounter := 0
			for {
				select {
				case trx := <-ReqChan:
					switch trx.Cmd {
					case "INSERT", "UPDATE":
						//log.Printf("insert/update record %s %d", trx.TableName, trx.ID)
						lastest, buf := GetData(trx)
						var resp *Response
						if buf != nil {
							//todo save to db
							time.Sleep(100 * time.Millisecond) //模拟数据库插入耗时

							counter++

							resp = &Response{Code: "OK", DBName:trx.DBName, TableName: trx.TableName, UID: trx.UID, SavedVersion: lastest, SavedStamp: time.Now().Unix()}
						} else {
							resp = &Response{Code: "SKIP", DBName:trx.DBName,TableName: trx.TableName, UID: trx.UID, SavedVersion: trx.Version, SavedStamp: time.Now().Unix()}
						}
						PutResp(resp)

					case "DELETE":
						log.Printf("delete record %d", trx.UID)
						resp := &Response{Code: "OK", DBName:trx.DBName,TableName: trx.TableName, UID: trx.UID, SavedVersion: trx.Version, SavedStamp: time.Now().Unix()}
						PutResp(resp)

					}
				case <-ticker.C:
					if counter != printedCounter {
						log.Printf("=======update db %d times======", counter)
						printedCounter = counter
					}
				}
			}
		}()

	}
}

func init() {
	work()
}
