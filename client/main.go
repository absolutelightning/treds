package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

const DefaultPort = "7997"

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
	// Connect to Treds server at localhost:7997
	portFlag := flag.String("port", DefaultPort, "Port to connect on")
	flag.Parse()

	host := os.Getenv("TREDS_HOST")
	port := os.Getenv("TREDS_PORT")
	if host == "" {
		host = "localhost"
	}
	if port == "" {
		port = "7997"
	}
	
	if portFlag != nil && *portFlag != "" {
		port = *portFlag
	}

	conn, err := connectToTreds(fmt.Sprintf("%s:%s", host, port))
	if err != nil {
		fmt.Println("Error connecting to Treds:", err)
		os.Exit(1)
	}
	defer conn.Close()

	// Start the CLI loop
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Connected to Treds. Type commands and press Enter.")
	for {
		fmt.Print("> ")
		cmd, _ := reader.ReadString('\n')   // Read command from user input
		cmd = strings.TrimSpace(cmd)        // Remove leading/trailing whitespace
		if cmd == "exit" || cmd == "quit" { // Allow user to quit
			break
		}

		// Measure the start time
		startTime := time.Now()

		// Send command to Treds
		err := sendCommand(conn, cmd)
		if err != nil {
			fmt.Println("Error sending command:", err)
			continue
		}

		// Read response from Treds
		response, err := readAllData(conn)
		if err != nil {
			fmt.Println("Error reading response:", err)
			continue
		}
		// Print response
		fmt.Println(response)

		// Calculate the elapsed time
		elapsedTime := time.Since(startTime)

		// Print the time taken
		fmt.Printf("Time taken: %v\n", elapsedTime)
	}

	fmt.Println("Disconnected from Treds.")
}
