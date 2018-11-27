package main

import (
	"log"
	"github.com/helloshiki/ndb"
)

type Transaction = ndb.Transaction
type Response struct {
	DBName string
	TableName    string
	Code         string
	UID           int
	SavedVersion uint64
	SavedStamp   int64
}

func putTrx(trx *Transaction) {
	select {
	case ReqChan <- trx:
	default:
		log.Printf("reqChain is full")
	}
}

func GetTrx() *Transaction {
	return <-ReqChan
}

func PutResp(res *Response) {
	select {
	case RespChan <- res:
	default:
		log.Printf("respChain is full")
	}
}

func getResp() *Response {
	return <-RespChan
}

func Work() {
	go func() {
		for {
			resp := getResp()
			switch resp.Code {
			case "OK":
				updateSavedVersion(resp)
				log.Printf("table %s's record %d, version %d done at %d", resp.TableName, resp.UID, resp.SavedVersion, resp.SavedStamp)
			case "SKIP":
				//log.Printf("table %s's record %d, version %d skipped at %d", resp.TableName, resp.ID, resp.SavedVersion, resp.SavedStamp)
			}
		}
	}()
}

func GetData(trx *Transaction) (uint64, []byte) { //return latest version data
	db := ndb.GetDB(trx.DBName)
	table := db.GetTable(trx.TableName)
	return table.GetRowBytes(trx.UID, trx.Version)
}

func updateSavedVersion(resp *Response) {
	db := ndb.GetDB(resp.DBName)
	table := db.GetTable(resp.TableName)
	table.UpdateSavedVersion(resp.UID, resp.SavedVersion)
}

var ReqChan chan *Transaction
var RespChan chan *Response

func init() {
	ndb.SetPutTx(putTrx)
	ReqChan = make(chan *Transaction, 100*1024)
	RespChan = make(chan *Response, 100*1024)
}
