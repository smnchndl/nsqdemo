package main

import (
	"flag"
	"github.com/nsqio/go-nsq"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
)

var start = time.Now()
var ops uint64 = 0
var numbPtr = flag.Int("msg", 100, "number of messages (default: 10000)")

func main() {

	quitChannel := make(chan os.Signal) // channel to capture os signal, used for consumer process to exit if interrupt and Terminate signal comes.
	signal.Notify(quitChannel, syscall.SIGINT, syscall.SIGTERM) //Interrupt and Terminate

	config := nsq.NewConfig()
	q, _ := nsq.NewConsumer("India_test", "ch", config)

	
	q.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error { // Handler function
		log.Println( "Message Body :::: ",string(message.Body))

		atomic.AddUint64(&ops, 1)  // maintaining counter for msg count
		log.Printf("ops ::::: ",ops)
		if ops == uint64(*numbPtr) {
			elapsed := time.Since(start)
			log.Printf("Time took %s", elapsed) // time measurement
		}

		return nil
	}))


	err := q.ConnectToNSQLookupd("127.0.0.1:4161") // for msg lookup

	if err != nil {
		log.Fatal("Could not connect")
	}

// It quits only when we get Interrupt and Terminal Signal

	<-quitChannel

	log.Println("Got quit, Closing Consumer....")

	log.Println("Done.")
}
