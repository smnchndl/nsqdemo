package main

import (
	"flag"
	"fmt"
	"log"
	"github.com/itmarketplace/go-queue"
	"github.com/nsqio/go-nsq"
	"runtime"
	"encoding/json"
)

type Data struct {
    Name string `json:"name"`
    Id int64 `json:"id"`
    Gender string `json:"gender"`
}

var numbPtr = flag.Int("msg", 100, "number of messages (default: 100)")


func FuncSimulateError(m *nsq.Message) error {
        data := Data{} 
        log.Println("Got a Message:::::::::", string(m.Body))
        err := json.Unmarshal(m.Body, &data) 

        if err != nil { 
                return err 
        } 

        if !(data.Gender != "") {
        panic("Gender must be a present.")
        }
        
        return nil 
}

func main() {

	flag.Parse()

	c := queue.NewConsumer("India_Json", "ch")

	c.Set("nsqlookupd", ":4161")
	c.Set("concurrency", runtime.GOMAXPROCS(runtime.NumCPU()))
	c.Set("max_attempts", 100)
	c.Set("max_in_flight", 150)
	c.Set("default_requeue_delay", "30s")

	c.Start(nsq.HandlerFunc(FuncSimulateError))

	fmt.Scanln()

	c.Stop()
}
