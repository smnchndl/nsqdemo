package main

import (
	"flag"
	"fmt"
	"github.com/nsqio/go-nsq"
	"log"
	"math/rand"
	"time"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!@#$%^&*()1234567890")
var numbPtr = flag.Int("msg", 10, "number of messages (default: 10000)") // to read command line arguments `msg` , take 1000 if nothing gives in msg


func randSeq(n int) string { // to generate random messages
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func main() {


	config := nsq.NewConfig()
	config.MaxInFlight = 100

	w, _ := nsq.NewProducer("127.0.0.1:4150", config) // we can use lib to get IP as well. [ lib.GetIPAddress() ] 

	flag.Parse()

	start := time.Now()

	var err error
	for i := 1; i <= *numbPtr; i++ {
		err = w.Publish("India_test", []byte(randSeq(30)))
		if err != nil {
			log.Println(err)
		}
	}

	elapsed := time.Since(start)
	log.Printf("Time took %s", elapsed)

	w.Stop()

	fmt.Scanln()
}
