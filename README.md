# tiny-cdc
---------------------------------------
checkpoint.data
{"binName":"111","binPos":123}

---------------------------------------
checkpoint.go
package common

import (
	"encoding/json"
	"github.com/siddontang/go-log/log"
	"os"
)

const CHECK_PONIT_FILE = "checkpoint.data"

// const CHECK_PONIT_FILE = "C:\\Users\\yicai.liu\\GolandProjects\\ds\\checkpoint.data"
type ConsumeCheckpoint struct {
	StartName string `json:"binName"`
	StartPos  uint32 `json:"binPos"`
}

var CurLogName string = ""

func GetCheckpoint() *ConsumeCheckpoint {
	data, err := os.ReadFile(CHECK_PONIT_FILE)
	if err != nil {
		log.Fatal("read checkpoint error")
	}
	var checkpoint *ConsumeCheckpoint
	err = json.Unmarshal(data, &checkpoint)
	if err != nil {
		log.Fatal("read checkpoint error")
		return nil
	}
	CurLogName = checkpoint.StartName
	return checkpoint
}

func NewCheckPoint() {
	checkpoint := ConsumeCheckpoint{
		StartName: "111",
		StartPos:  123,
	}

	data, _ := json.Marshal(&checkpoint)

	err := os.WriteFile(CHECK_PONIT_FILE, data, os.ModePerm)
	if err != nil {
		log.Fatal("write checkpoint error")
		return
	}
}

---------------------------------------

main.go
package main

import (
	"ds/common"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"
	"github.com/siddontang/go-log/log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type CDCEvent struct {
	Action     string
	Schema     string
	Table      string
	Time       time.Time
	BatchNo    string
	PKValue    interface{}
	Checkpoint common.ConsumeCheckpoint
}

type CDCEventHandler struct {
	canal.DummyEventHandler
}

// key batchNo, value batch data event
var eventStream = make(chan *CDCEvent)

func (h *CDCEventHandler) OnRotate(rh *replication.EventHeader, re *replication.RotateEvent) error {
	log.Info("On rotate...")
	return nil
}

func (h *CDCEventHandler) OnRow(e *canal.RowsEvent) error {

	/*if (canal.InsertAction != e.Action) || (canal.UpdateAction != e.Action) || (canal.DeleteAction != e.Action) {
		return nil
	}*/
	table := e.Table
	pkColumns := table.PKColumns
	if len(pkColumns) <= 0 {
		log.Infof("%s :no pk found", table.Name)
		return nil
	}
	pkIndex := pkColumns[0]
	rows := e.Rows
	batchNo := uuid.New().String()
	for i := 0; i < len(rows); i++ {
		if canal.UpdateAction == e.Action && i%2 == 0 {
			continue
		}
		pkVal := rows[i][pkIndex]

		event := &CDCEvent{
			Time:    time.Now(),
			Action:  e.Action,
			Schema:  table.Schema,
			Table:   table.Name,
			BatchNo: batchNo,
			PKValue: pkVal,
			Checkpoint: common.ConsumeCheckpoint{
				StartName: common.CurLogName,
				StartPos:  0,
			},
		}
		eventStream <- event
	}
	return nil
}

func (h *CDCEventHandler) String() string {
	return "CDCEventHandler"
}

func main() {

	// consumer
	go func() {
		for {
			data, ok := <-eventStream
			if !ok {
				break
			}
			log.Infof("batchEvent :%v\n", data)
		}
	}()

	// producer
	checkpoint := common.GetCheckpoint()
	startPos := mysql.Position{
		Name: checkpoint.StartName,
		Pos:  checkpoint.StartPos,
	}
	c := prepareCanal()
	go func() {
		err := c.RunFrom(startPos)
		if err != nil {
			fmt.Printf("start canal err %v", err)
		}
	}()

	quit := make(chan os.Signal)
	signal.Notify(quit,
		os.Kill,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	<-quit

	c.Close()
}

func prepareCanal() *canal.Canal {

	cfg := canal.NewDefaultConfig()
	cfg.Addr = "127.0.0.1:3306"
	cfg.User = "root"
	cfg.Password = "123456"
	// We only care table canal_test in test db
	cfg.Dump.TableDB = "wuxin"
	cfg.Dump.Tables = []string{"test1"}

	c, err := canal.NewCanal(cfg)
	if err != nil {
		log.Fatal(err)
	}

	// Register a handler to handle RowsEvent
	c.SetEventHandler(&CDCEventHandler{})
	return c
}
