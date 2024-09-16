# Treds - Sorted Data Structure Server  [![Run CI Tests](https://github.com/absolutelightning/treds/actions/workflows/go.yml/badge.svg)](https://github.com/absolutelightning/treds/actions/workflows/go.yml)

This is a Radix Trie Based Data Structure Server in which keys are stored in sorted order.
Scan of keys returns the keys in sorted order. 

## How it is different from Redis?
* Keys at root level having a common prefix can be queried optimally
* `SCANKEYS/SCANKVS/KEYS/KVS` commands returns the results in sorted order
* Unlike [Redis SCAN](https://redis.io/docs/latest/commands/scan/) it **always** returns count number of data if data is there
* It has Sorted Maps instead of Sorted Sets. So we can create a Sorted Kev/Value pair with associated with a score
* Currently, it only has Key/Value store and Sorted Maps store and only supports strings/integers as values

## Internals

It is single threaded and has event loop.
Implemented using modified Radix trees where leaf nodes are connected by Doubly Linked List in Radix Trie to facilitate the quick lookup of keys/values in sorted order.
Doubly Linked List of leaf nodes are updated at the time of create/delete and update of keys optimally.
This structure is similar to [Prefix Hash Tree](https://people.eecs.berkeley.edu/~sylvia/papers/pht.pdf), but without converting keys to binary.
Tree Map used to store score maps also are connected internally using Doubly Linked List using similar logic

## Commands 
* `PING` - Replies with a `PONG`
* `SET key value` - Sets a key value pair
* `GET key` - Get a value for a key
* `DEL key` - Delete a key
* `MSET key1 value1 [key2 value2 key3 value3 ....]`- Set values for multiple keys
* `MGET key1 [key2 key3 ....]`- Get values for multiple keys
* `DELPREFIX prefix` - Delete all keys having a common prefix
* `DBSIZE` - Get number of keys in the db
* `SCANKEYS cursor prefix count` - Returns the count number of keys matching prefix starting from an index in lex order. Last element is the next cursor
* `SCANKVS cursor prefix count` - Returns the count number of keys/value pair in which keys match prefix starting from an index in lex order. Last element is the next cursor
* `KEYS regex` - Returns all keys matching a regex in lex order - (Not suitable to production use cases with huge number of keys)
* `KVS regex` - Returns all keys/values in which keys match a regex in lex order - (Not suitable to production use cases with huge number of keys)
* `ZADD key score member_key member_value [member_key member_value ....]` - Add member_key with member value with score to a sorted map in key
* `ZREM key member [member..]` - Removes a member from sorted map in key
* `ZCARD key` - Returns the count of key/value pairs in sorted map in key
* `ZSCORE key member` - Returns the score of a member in sorted map in key
* `ZRANGELEXKEYS key offset count withscore prefix` - Returns the count number of keys matching prefix starting from an index in a sorted map in lex order. WithScore can be true or false
* `ZRANGELEXKVS key offset count withscore prefix` - Returns the count number of key/value pair in which keys match prefix starting from an index in a sorted map in lex order. WithScore can be true or false
* `ZRANGESCOREKEYS key min max offset count withscore` - Returns the count number of keys with the score between min/max in sorted order of score. WithScore can be true or false
* `ZRANGESCOREKVS key min max offset count withscore` - Returns the count number of key/value pair with the score between min/max in sorted order of score. WithScore can be true or false
* `ZREVRANGELEXKEYS key offset count withscore prefix` - Returns the count number of keys matching prefix starting from an index in a sorted map in reverse lex order. WithScore can be true or false
* `ZREVRANGELEXKVS key offset count withscore prefix` - Returns the count number of key/value pair in which keys match prefix starting from an index in a sorted map in reverse lex order. WithScore can be true or false
* `ZREVRANGESCOREKEYS key min max offset count withscore` - Returns the count number of keys with the score between min/max in reverser sorted order of score. WithScore can be true or false
* `ZREVRANGESCOREKVS key min max offset count withscore` - Returns the count number of key/value pair with the score between min/max in reverse sorted order of score. WithScore can be true or false

## Run 

To run server run the following command on repository root

```text
go run main.go 
```

For CLI run the following command in the `client` folder in the repo

```text
cd ./client
go run main.go 
```

## Future Work
* Add Raft for HA
