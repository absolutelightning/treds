package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"treds/server"

	"github.com/panjf2000/gnet/v2"
)

const DefaultPort = "7997"

func main() {
	portFlag := flag.String("port", DefaultPort, "Port at which server will listen")

	flag.Parse()

	var sigs = make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)

	port := os.Getenv("TREDS_PORT")

	if len(port) == 0 {
		port = DefaultPort
	}

	if portFlag != nil && *portFlag != "" {
		port = *portFlag
	}

	portInt, err := strconv.Atoi(port)

	if err != nil {
		panic(err)
	}

	tredsServer, err := server.New(portInt)
	if err != nil {
		log.Fatal(err)
	}

	log.Fatal(gnet.Run(
		tredsServer,
		"tcp://0.0.0.0:"+strconv.Itoa(tredsServer.Port),
		gnet.WithMulticore(false),
		gnet.WithReusePort(false),
		gnet.WithTCPKeepAlive(300*time.Second),
	))

}
