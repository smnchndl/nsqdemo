package main

import (
	"flag"
	"fmt"
	"github.com/nsqio/go-nsq"
	"log"
	"math/rand"
	"time"
	"encoding/json"
)


type Message struct {
    Name string  `json:"name"`
    Id int `json:"id"`
    Designation string `json:"designation"`
}


var names = []string{// created an array of strings holding names
    "Jerry",
    "Shinchan",
    "Tommy",
    "Motu"} 

var ids =[]int {1,2,3,4,5,6,8,9,7}

var  designations = []string{// created an array of strings holding designations
    "Software Engineer",
    "Sr. Software Engineer",
    "Tech Lead",
    "Sr. Tech Lead"} 

var numbPtr = flag.Int("msg", 10, "number of messages (default: 10000)") // to read command line arguments `msg` , take 1000 if nothing gives in msg


func main() {


	config := nsq.NewConfig()
	config.MaxInFlight = 100

	w, _ := nsq.NewProducer("127.0.0.1:4150", config) // we can use lib to get IP as well. [ lib.GetIPAddress() ] 

	flag.Parse()

	start := time.Now()

	var err error
	for i := 1; i <= *numbPtr; i++ {

        nam :=names[rand.Int() % len(names)]
        // log.Println("Names::::",nam)
		id := ids[rand.Int() % len(ids)]
		// log.Println("ids::::",id)
		des := designations[rand.Int() % len(designations)]
		// log.Println("Designation::::",des)
		m:= Message{nam, id, des}
        b, _ := json.Marshal(m)
		err = w.Publish("India_Json", b)
		if err != nil {
			log.Println(err)
		} 
	}

	elapsed := time.Since(start)
	log.Printf("Time took %s", elapsed)

	w.Stop()

	fmt.Scanln()
}
