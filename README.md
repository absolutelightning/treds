# TREDS - Radix Tree Based Data Structure Server  [![Run CI Tests](https://github.com/absolutelightning/treds/actions/workflows/go.yml/badge.svg)](https://github.com/absolutelightning/treds/actions/workflows/go.yml)

This is a Radix Trie Based Data Structure Server with primary focus of getting keys in sorted or quickly matching a common prefix.

It is single threaded and has event loop.

Implemented using modified Radix trees where leaf nodes are connected by Linkedlist in Radix Trie to faciliate the quick lookup of keys/values in sorted order.

LinkedList of leaf nodes are updated at the time of create/delete and udpate of keys optimally.

## Commands 
* `set key value` - Sets a key value pair
* `get key` - Get a value for a key
* `mget key1 key2`- Get values for multiple keys
* `scankeys startindex prefix count` - Returns the count number of keys matching prefix starting from an index 
* `scankvs startindex prefix count` - Returns the count number of keys/value pair in which keys match prefix starting from an index
* `keys regex` - Returns all keys matching a regex - (Not suitable to production usecases with huge number of keys)
* `kvs regex` - Returns all keys/values in which keys match a regex - (Not suitable to production usecases with huge number of keys)
* `zadd key score member_key member_value [member_key member_value ....]` - Add member_key with member value with score to a sorted map in key
* `zrangelexkeys key startindex prefix count` - Returns the count number of keys matching prefix starting from an index in a sorted map  
* `zrangelexkvs key startindex prefix count` - Returns the count number of key/value pair n which keys match prefix starting from an index in a sorted map

## Future Work
* Add Raft for HA
