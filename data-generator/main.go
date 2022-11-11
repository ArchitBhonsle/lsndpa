package main

import (
	"bytes"
	"encoding/binary"
	"log"
	"math"
	"math/rand"
	"net/http"
	"time"
)

const addr = "http://localhost:8000/"
const metersInLat = 111320.0
const delta = 5.0

func randomInRange(min, max float64) float64 {
	return min + (max-min)*rand.Float64()
}

func spawnUser(id uint64, exit chan string) {
	latitude := randomInRange(-90.0, 90.0)
	longitude := randomInRange(-180.0, 180.0)

	timer := time.NewTimer(delta * time.Second)

	for {
		timestamp := time.Now().UnixNano()

		buf := make([]byte, 32)
		binary.BigEndian.PutUint64(buf[0:], id)
		binary.BigEndian.PutUint64(buf[8:], uint64(timestamp))
		binary.BigEndian.PutUint64(buf[16:], math.Float64bits(latitude))
		binary.BigEndian.PutUint64(buf[24:], math.Float64bits(longitude))

		resp, err := http.Post(addr, "application/octet-stream", bytes.NewReader(buf))
		if err != nil {
			log.Panic(err)
		}
		defer resp.Body.Close()

		log.Println("request sent")

		latitude += delta / metersInLat
		<-timer.C
		timer.Reset(delta * time.Second)
	}
}

func main() {
	exit := make(chan string)

	for id := 0; id < 1; id++ {
		go spawnUser(uint64(id), exit)
	}

	<-exit
}
