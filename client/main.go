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

	"github.com/c-bata/go-prompt"
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

func completer(d prompt.Document) []prompt.Suggest {
	input := d.TextBeforeCursor()
	firstWord := strings.Split(input, " ")[0]
	if firstWord == "" {
		return []prompt.Suggest{}
	}
	s := []prompt.Suggest{
		{Text: "DBSIZE", Description: "Get number of keys in the db"},
		{Text: "DEL", Description: "DEL key - Delete a key"},
		{Text: "DELPREFIX", Description: "DELPREFIX prefix - Delete all keys having a common prefix. Returns number of keys deleted"},
		{Text: "EXPIRE", Description: "EXPIRE key seconds - Expire key after given seconds"},
		{Text: "FLUSHALL", Description: "FLUSHALL - Deletes all keys"},
		{Text: "GET", Description: "GET key - Get a value for a key"},
		{Text: "HDEL", Description: "HDEL key field [field ...] - Deletes the fields present inside the hash at the key"},
		{Text: "HEXISTS", Description: "HEXISTS key field - Returns a true or false based on field is present in hash at key or not"},
		{Text: "HGET", Description: "HGET key field - Returns the value present at field inside the hash at key"},
		{Text: "HGETALL", Description: "HGETALL key - Returns all field value pairs inside the hash at the key"},
		{Text: "HKEYS", Description: "HKEYS key - Returns all field present in the hash at key"},
		{Text: "HLEN", Description: "HLEN key - Returns the size of hash at the key"},
		{Text: "HSET", Description: "HSET key field value [field value ...] - Sets field value pairs in the hash with key"},
		{Text: "HVALS", Description: "HVALS key - Returns all values present in the hash at key"},
		{Text: "KEYS", Description: "KEYS cursor regex count - Returns count number of keys matching a regex in lex order starting with cursor. Count is optional. Last element is the next cursor"},
		{Text: "KVS", Description: "KVS cursor regex count - Returns count number of keys/values in which keys match a regex in lex order starting with cursor. Count is optional. Last element is the next cursor"},
		{Text: "LINDEX", Description: "LINDEX key index - Returns the element at index of list with key"},
		{Text: "LLEN", Description: "LLEN key - Returns the length of list with key"},
		{Text: "LNGPREFIX", Description: "LNGPREFIX string - Returns the key value pair in which key is the longest prefix of given string"},
		{Text: "LPOP", Description: "LPOP key count - Removes count elements from left of list with key and returns the popped elements"},
		{Text: "LPUSH", Description: "LPUSH key element [element ...] - Adds elements to the left of list with key"},
		{Text: "LRANGE", Description: "LRANGE key start stop - Returns the elements from start index to stop index in the list with key"},
		{Text: "LREM", Description: "LREM key index - Removes element at index of list with key"},
		{Text: "LSET", Description: "LSET key index element - Sets an element at an index of a list with key"},
		{Text: "MGET", Description: "MGET key1 [key2 key3 ....]- Get values for multiple keys"},
		{Text: "MSET", Description: "MSET key1 value1 [key2 value2 key3 value3 ....]- Set values for multiple keys"},
		{Text: "PING", Description: "PING - Replies with a PONG"},
		{Text: "RPOP", Description: "RPOP key count - Removes count elements from right of list with key and returns the popped elements"},
		{Text: "RPUSH", Description: "RPUSH key element [element ...] - Adds elements to the right of list with key"},
		{Text: "SADD", Description: "SADD member [member ...] - Adds the members to a set with key"},
		{Text: "SCANKEYS", Description: "SCANKEYS cursor prefix count - Returns the count number of keys matching prefix starting from an index in lex order only present in Key/Value Store. Last element is the next cursor"},
		{Text: "SCANKVS", Description: "SCANKVS cursor prefix count - Returns the count number of keys/value pair in which keys match prefix starting from an index in lex order only present in Key/Value Store. Last element is the next cursor"},
		{Text: "SCARD", Description: "SCARD key - Returns the size of the set with key"},
		{Text: "SDIFF", Description: "SDIFF key [key ...] - Returns the difference between the first set and all the successive sets"},
		{Text: "SET", Description: "SET key value - Sets a key value pair"},
		{Text: "SINTER", Description: "SINTER key [key ...] - Returns the intersection of sets with the given keys"},
		{Text: "SISMEMBER", Description: "SISMEMBER key member - Return 1 if member is present in set with key, 0 otherwise"},
		{Text: "SMEMBERS", Description: "SMEMBERS key - Returns all members of a set with key"},
		{Text: "SREM", Description: "SREM member [member ...] - Removes the members from a set with key"},
		{Text: "SUNION", Description: "SUNION key [key ...] - Returns the union of sets with the give keys"},
		{Text: "TTL", Description: "TTL key - Returns the time in seconds remaining before key expires. -1 if key has no expiry, -2 if key is not present"},
		{Text: "ZADD", Description: "ZADD key score member_key member_value [member_key member_value ....] - Add member_key with member value with score to a sorted map in key"},
		{Text: "ZCARD", Description: "ZCARD key - Returns the count of key/value pairs in sorted map in key"},
		{Text: "ZRANGELEXKEYS", Description: "ZRANGELEXKEYS key offset count withscore min max - Returns the count number of keys are greater than min and less than max starting from an index in a sorted map in lex order. WithScore can be true or false"},
		{Text: "ZRANGELEXKVS", Description: "ZRANGELEXKVS key offset count withscore min max - Returns the count number of key/value pair in which keys are greater than min and less than max starting from an index in a sorted map in lex order. WithScore can be true or false"},
		{Text: "ZRANGESCOREKEYS", Description: "ZRANGELEXKVS key offset count withscore min max - Returns the count number of key/value pair in which keys are greater than min and less than max starting from an index in a sorted map in lex order. WithScore can be true or false"},
		{Text: "ZRANGESCOREKVS", Description: "ZRANGESCOREKVS key min max offset count withscore - Returns the count number of key/value pair with the score between min/max in sorted order of score. WithScore can be true or false"},
		{Text: "ZREM", Description: "ZREM key member [member ...] - Removes a member from sorted map in key"},
		{Text: "ZREVRANGELEXKEYS", Description: "ZREVRANGELEXKEYS key offset count withscore min max - Returns the count number of keys are greater than min and less than max starting from an index in a sorted map in reverse lex order. WithScore can be true or false"},
		{Text: "ZREVRANGELEXKVS", Description: "ZREVRANGELEXKVS key offset count withscore min max - Returns the count number of key/value pair in which keys are greater than min and less than max starting from an index in a sorted map in reverse lex order. WithScore can be true or false"},
		{Text: "ZREVRANGESCOREKEYS", Description: "ZREVRANGESCOREKEYS key min max offset count withscore - Returns the count number of keys with the score between min/max in reverser sorted order of score. WithScore can be true or false"},
		{Text: "ZREVRANGESCOREKVS", Description: "ZREVRANGESCOREKVS key min max offset count withscore - Returns the count number of key/value pair with the score between min/max in reverse sorted order of score. WithScore can be true or false"},
		{Text: "ZSCORE", Description: "ZSCORE key member - Returns the score of a member in sorted map in key"},
	}
	return prompt.FilterHasPrefix(s, firstWord, true)
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
	fmt.Println("Connected to Treds. Type commands and press Enter.")
	fmt.Println("Please use `Ctrl-D` to exit this program.")
	defer fmt.Println("Bye!")
	p := prompt.New(
		func(cmd string) {
			if cmd == "" || cmd == "\n" {
				return
			}
			// Measure the start time
			startTime := time.Now()
			// Send command to Treds
			err = sendCommand(conn, cmd)
			if err != nil {
				fmt.Println("Error sending command:", err)
				return
			}
			// Read response from Treds
			response, rerr := readAllData(conn)
			if rerr != nil {
				fmt.Println("Error reading response:", err)
				return
			}
			// Print response
			fmt.Println(response)
			// Calculate the elapsed time
			elapsedTime := time.Since(startTime)
			// Print the time taken
			fmt.Printf("Time taken: %v\n", elapsedTime)
		},
		completer,
		prompt.OptionPrefix(">>> "),
		prompt.OptionPrefixTextColor(prompt.Yellow),
		prompt.OptionSuggestionTextColor(prompt.Yellow),
		prompt.OptionSuggestionBGColor(prompt.Black),
		prompt.OptionDescriptionBGColor(prompt.Black),
		prompt.OptionDescriptionTextColor(prompt.Yellow),
		prompt.OptionScrollbarBGColor(prompt.Black),
	)
	p.Run()
}
