# TREDS - Radix Tree Based Data Structure Server  [![Run CI Tests](https://github.com/absolutelightning/treds/actions/workflows/go.yml/badge.svg)](https://github.com/absolutelightning/treds/actions/workflows/go.yml)

This is a Radix Trie Based Data Structure Server with primary focus of getting keys in sorted or quickly matching a common prefix.

It is single threaded and has event loop.

Implemented using modified Radix trees where leaf nodes are connected by Linkedlist in Radix Trie to faciliate the quick lookup of keys/values in sorted order.

LinkedList of leaf nodes are updated at the time of create/delete and udpate of keys optimally.

## Commands 
* `SET key value` - Sets a key value pair
* `GET key` - Get a value for a key
* `MGET key1 key2`- Get values for multiple keys
* `SCANKEYS startindex prefix count` - Returns the count number of keys matching prefix starting from an index
* `SCANKVS startindex prefix count` - Returns the count number of keys/value pair in which keys match prefix starting from an index
* `KEYS regex` - Returns all keys matching a regex - (Not suitable to production usecases with huge number of keys)
* `KVS regex` - Returns all keys/values in which keys match a regex - (Not suitable to production usecases with huge number of keys)
* `ZADD key score member_key member_value [member_key member_value ....]` - Add member_key with member value with score to a sorted map in key
* `ZREM key member [member..]` - Removes a member from sorted map in key
* `ZCARD key` - Returns the count of key/value pairs in sorted map in key
* `ZSCORE key member` - Returns the score of a member in sorted map in key
* `ZRANGELEXKEYS key startindex prefix count` - Returns the count number of keys matching prefix starting from an index in a sorted map
* `ZRANGELEXKVS key startindex prefix count` - Returns the count number of key/value pair in which keys match prefix starting from an index in a sorted map
* `ZRANGESCOREKEYS key min max startindex count withscore` - Returns the count number of keys with the score between min/max in sorted order
* `ZRANGESCOREKVS key min max startindex count withscore` - Returns the count number of key/value pair with the score between min/max in sorted order

## Run 

To run server run the following command on repository root

```text
go run main.go 
```

For cli run the following command in the `client` folder in the repo

```text
go run main.go 
```

## Future Work
* Add Raft for HA
