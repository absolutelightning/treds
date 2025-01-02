package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"treds/server"

	"github.com/panjf2000/gnet/v2"
)

const DefaultPort = "7997"
const DefaultBind = "localhost"
const DefaultAdvertise = "localhost"
const DefaultSegmentSize = 200

func parseServers(input string) []server.BootStrapServer {
	if input == "" {
		return nil
	}

	var servers []server.BootStrapServer
	serverEntries := strings.Split(input, ",")

	for _, entry := range serverEntries {
		parts := strings.Split(entry, ":")
		if len(parts) != 3 {
			fmt.Printf("Error: Invalid server format '%s'. Skipping...\n", entry)
			continue
		}

		port := 0
		_, err := fmt.Sscanf(parts[2], "%d", &port)
		if err != nil {
			return nil
		}

		servers = append(servers, server.BootStrapServer{
			ID:   parts[0],
			Host: parts[1],
			Port: port,
		})
	}

	return servers
}

func main() {
	serverId := flag.String("id", "", "Server Id - must be a uuid, if not given a new one will be generated")
	portFlag := flag.String("port", DefaultPort, "Port at which server will listen")
	segmentSize := flag.Int("segmentSize", DefaultSegmentSize, "Segment size")
	bindAddr := flag.String("bind", DefaultBind, "Bind Address")
	advertiseAddr := flag.String("advertise", DefaultAdvertise, "Advertise Address")
	applyTimeout := flag.Duration("raftApplyTimeout", 1*time.Second, "Raft Apply Timeout")
	servers := flag.String("servers", "", "Comma-separated list of servers in the format id:host:port (e.g., 'uuid1:127.0.0.1:8080,uuid2:192.168.1.1:9090')")

	flag.Parse()

	serverList := parseServers(*servers)

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

	tredsServer, err := server.New(portInt, *segmentSize, *bindAddr, *advertiseAddr, *serverId, *applyTimeout, serverList)
	if err != nil {
		log.Fatal(err)
	}

	log.Fatal(gnet.Run(
		tredsServer,
		"tcp://0.0.0.0:"+strconv.Itoa(tredsServer.Port),
		gnet.WithMulticore(true),
		gnet.WithReusePort(false),
		gnet.WithTCPKeepAlive(300*time.Second),
	))

}
