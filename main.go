package main

import (
	"dserv/server"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
)

const DefaultPort = "7997"

func main() {
	var sigs chan os.Signal = make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	var wg sync.WaitGroup
	wg.Add(1)

	port := os.Getenv("TREDS_PORT")

	if len(port) == 0 {
		port = DefaultPort
	}

	portInt, err := strconv.Atoi(port)

	if err != nil {
		panic(err)
	}

	// numLoops should always be 1 because datastructures do not support MVCC.
	// This is single threaded operation
	trieDataStructureServer := server.New(portInt)

	go trieDataStructureServer.Init()
	go func() {
		for err := range trieDataStructureServer.ErrCh {
			log.Printf("error running server " + err.Error())
		}
	}()

	wg.Wait()
}
