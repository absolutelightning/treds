package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"time"
)

// Connect to the Treds server
func connectToTreds(address string) (net.Conn, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// Send a command to Treds server in RESP format
func sendCommand(conn net.Conn, command string) error {
	// Convert the command into RESP format
	_, err := conn.Write([]byte(command))
	return err
}

// Function to read from the connection until a newline character
func readUntilNewline(reader *bufio.Reader) (string, error) {
	// Read until '\n'
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return line, nil
}

func readFixedBytes(reader *bufio.Reader, n int) ([]byte, error) {
	// Create a buffer of size n to hold the data
	buffer := make([]byte, n)

	// Read exactly n bytes into the buffer
	_, err := io.ReadFull(reader, buffer)
	if err != nil {
		return nil, err
	}
	return buffer, nil
}

// Read all data from the server
func readAllData(conn net.Conn) (string, error) {
	reader := bufio.NewReader(conn)

	length, err := readUntilNewline(reader)
	if err != nil {
		return "", err
	}
	lengthInt, err := strconv.Atoi(length[:len(length)-1])
	if err != nil {
		return "", err
	}

	resp, err := readFixedBytes(reader, lengthInt)
	if err != nil {
		return "", err
	}
	return string(resp), nil
}

func main() {
	timeDuration := scan(0)
	fmt.Println(fmt.Sprintf("user:* -> %v", timeDuration))
	for i := 1; i <= 10000000; i *= 10 {
		timeDuration = scan(i)
		fmt.Println(fmt.Sprintf("user:%d* -> %v", i, timeDuration))
	}
}

func scan(i int) *time.Duration {
	// Connect to Treds server at localhost:7997
	host := os.Getenv("TREDS_HOST")
	port := os.Getenv("TREDS_PORT")
	if host == "" {
		host = "localhost"
	}
	if port == "" {
		port = "7997"
	}
	conn, err := connectToTreds(fmt.Sprintf("%s:%s", host, port))
	if err != nil {
		fmt.Println("Error connecting to Treds:", err)
		os.Exit(1)
	}

	command := fmt.Sprintf("scankeys 0 user:%d 100000000000", i)

	if i == 0 {
		command = fmt.Sprintf("scankeys 0 user: 100000000000")
	}

	startTime := time.Now()

	// Send command to Treds
	err = sendCommand(conn, command)

	if err != nil {
		fmt.Println("Error:", err)
		return nil
	}

	response, err := readAllData(conn)

	fmt.Println(response)

	endTime := time.Since(startTime)

	return &endTime
}
