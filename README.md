# TREDS - Radix Trie Based Data Structure Server
This is a Radix Trie Based Data Structure Server with primary focus of getting keys in sorted or quickly
Leaf nodes are connected by Linkedlist in Radix Trie to faciliate the quick lookup of keys/values in sorted order. 

## Comamnds 
* `set key value` - Sets a key value pair
* `get key` - Get a value for a key
* `mget key1 key2`- Get values for multiple keys
* `prefixscankeys startindex prefix count` - Returns the count number of keys matching prefix starting from an index 
* `prefixscan startindex prefix count` - Returns the count number of keys/value pair in which keys match prefix starting from an index

