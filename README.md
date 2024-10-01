# Treds - Sorted Data Structure Server  [![Run CI Tests](https://github.com/absolutelightning/treds/actions/workflows/go.yml/badge.svg)](https://github.com/absolutelightning/treds/actions/workflows/go.yml)

Treds is a Radix Trie based data structure server that stores keys in sorted order, ensuring fast and efficient retrieval.
A scan operation returns keys in their sorted sequence.

## How it is different from Redis?
* Keys at root level having a common prefix can be queried optimally
* `SCANKEYS/SCANKVS/KEYS/KVS` commands returns the results in sorted order
* Unline [Redis KEYS](https://redis.io/docs/latest/commands/keys/), Treds `KEYS` has cursor and matches any valid regex experssion also it returns count number of data if data is there
* Unlike [Redis SCAN](https://redis.io/docs/latest/commands/scan/), Treds `SCAN` **always** returns count number of data if data is there
* Unlike [Redis ZRANGEBYLEX](https://redis.io/docs/latest/commands/zrangebylex/), Treds `ZRANGELEX` **always** returns data irrespective of score, basically data across different scores are returned
* It has Sorted Maps instead of Sorted Sets. So we can create a Sorted Key/Value pair with associated with a score
* New command - `DELPREFIX` - Deletes all keys having a common prefix and returns number of keys deleted
* Currently, it only has Key/Value store, Sorted Maps store, List store, Set store and Hash store and only supports strings/number as values

## Internals

It is single threaded and has event loop.
Implemented using modified Radix trees where leaf nodes are connected by Doubly Linked List in Radix Trie to facilitate the quick lookup of keys/values in sorted order.
Doubly Linked List of leaf nodes are updated at the time of create/delete and update of keys optimally.
This structure is similar to [Prefix Hash Tree](https://people.eecs.berkeley.edu/~sylvia/papers/pht.pdf), but for Radix Tree and without converting keys to binary.
Tree Map used to store score maps also are connected internally using Doubly Linked List using similar logic.
For more details - check out the [medium article](https://ashesh-vidyut.medium.com/optimizing-radix-trees-efficient-prefix-search-and-key-iteration-0c4fb817eac2)

## Performance Comparison
Both Treds and Redis are filled with 10 Million Keys in KeyValue Store and 10 Million Keys in a Sorted Map/Set respectively
Each key is of format `user:%d`, so every key has prefix `user:`
The commands are run in Golang program and redirecting the output to a file `go run main.go > out`.
For Redis setup see - [Redis Prefix Bench Repo](https://github.com/absolutelightning/redis-prefix-bench)

### Treds - ScanKeys vs Redis - Scan

Treds Command -
```text
scankeys 0 prefix 100000000000
```

Redis Command - 
```text
scan 0 match prefix count 100000000000 
```
This graph shows the performance comparison between Treds - ScanKeys and Redis - Scan:

![ScanKeys Comparison](./benchmark/scan-comparison.png)

### Treds - ScanKVS vs RedisSearch FT.SEARCH

Treds Command -
```text
scankvs 0 prefix 1000
```

Redis Command -
```text
FT.SEARCH idx:user prefix SORTBY name LIMIT 0 1000
```

Prefix for redis command can be replaced by "User*", "User1*", "User10*" ... etc

This graph shows the performance comparison between Treds - ScanKVS and Redis FT.Search:

![ScanKVS Comparison](./benchmark/scankvs-comparision.png)

### Treds - ZRangeScoreKeys vs Redis - ZRangeByScore
Treds Command -
```text
zrangescorekeys key 0 max 0 100000000000 false
```

Redis Command -
```text
zrangebyscore key 0 max
```
This graph shows the performance comparison between Treds - ZRangeScoreKeys and Redis - ZRangeByScore:

![ZRangeScore Comparison](./benchmark/zrange-score-comparison.png)

## Commands 
* `PING` - Replies with a `PONG`
* `SET key value` - Sets a key value pair
* `GET key` - Get a value for a key
* `DEL key` - Delete a key
* `MSET key1 value1 [key2 value2 key3 value3 ....]`- Set values for multiple keys
* `MGET key1 [key2 key3 ....]`- Get values for multiple keys
* `DELPREFIX prefix` - Delete all keys having a common prefix. Returns number of keys deleted
* `LNGPREFIX string` - Returns the key value pair in which key is the longest prefix of given string 
* `DBSIZE` - Get number of keys in the db
* `SCANKEYS cursor prefix count` - Returns the count number of keys matching prefix starting from an index in lex order only present in Key/Value Store. Last element is the next cursor
* `SCANKVS cursor prefix count` - Returns the count number of keys/value pair in which keys match prefix starting from an index in lex order only present in Key/Value Store. Last element is the next cursor
* `KEYS cursor regex count` - Returns count number of keys matching a regex in lex order starting with cursor. Count is optional. Last element is the next cursor
* `KVS cursor regex count` - Returns count number of keys/values in which keys match a regex in lex order starting with cursor. Count is optional. Last element is the next cursor
* `ZADD key score member_key member_value [score member_key member_value ....]` - Add member_key with member value with score to a sorted map in key
* `ZREM key member [member ...]` - Removes a member from sorted map in key
* `ZCARD key` - Returns the count of key/value pairs in sorted map in key
* `ZSCORE key member` - Returns the score of a member in sorted map in key
* `ZRANGELEXKEYS key offset count withscore min max` - Returns the count number of keys are >= min and <= max starting from an index in a sorted map in lex order. WithScore can be true or false
* `ZRANGELEXKVS key offset count withscore min max` - Returns the count number of key/value pair in which keys are >= min and <= max starting from an index in a sorted map in lex order. WithScore can be true or false
* `ZRANGESCOREKEYS key min max offset count withscore` - Returns the count number of keys with the score between min/max in sorted order of score. WithScore can be true or false
* `ZRANGESCOREKVS key min max offset count withscore` - Returns the count number of key/value pair with the score between min/max in sorted order of score. WithScore can be true or false
* `ZREVRANGELEXKEYS key offset count withscore min max` - Returns the count number of keys are >= min and <= max starting from an index in a sorted map in reverse lex order. WithScore can be true or false
* `ZREVRANGELEXKVS key offset count withscore min max` - Returns the count number of key/value pair in which keys are >= min and <= max starting from an index in a sorted map in reverse lex order. WithScore can be true or false
* `ZREVRANGESCOREKEYS key min max offset count withscore` - Returns the count number of keys with the score between min/max in reverser sorted order of score. WithScore can be true or false
* `ZREVRANGESCOREKVS key min max offset count withscore` - Returns the count number of key/value pair with the score between min/max in reverse sorted order of score. WithScore can be true or false
* `LPUSH key element [element ...]` - Adds elements to the left of list with key
* `RPUSH key element [element ...]` - Adds elements to the right of list with key
* `LPOP key count` - Removes count elements from left of list with key and returns the popped elements
* `RPOP key count` - Removes count elements from right of list with key and returns the popped elements
* `LREM key index` - Removes element at index of list with key
* `LSET key index element` - Sets an element at an index of a list with key
* `LRANGE key start stop` - Returns the elements from start index to stop index in the list with key
* `LLEN key` - Returns the length of list with key
* `LINDEX key index` - Returns the element at index of list with key
* `SADD member [member ...]` - Adds the members to a set with key
* `SREM member [member ...]` - Removes the members from a set with key
* `SMEMBERS key` - Returns all members of a set with key
* `SISMEMBER key member` - Return 1 if member is present in set with key, 0 otherwise
* `SCARD key` - Returns the size of the set with key
* `SUNION key [key ...]` - Returns the union of sets with the give keys
* `SINTER key [key ...]` - Returns the intersection of sets with the given keys
* `SDIFF key [key ...]` - Returns the difference between the first set and all the successive sets.
* `HSET key field value [field value ...]` - Sets field value pairs in the hash with key 
* `HGET key field` - Returns the value present at field inside the hash at key
* `HGETALL key` - Returns all field value pairs inside the hash at the key
* `HLEN key` - Returns the size of hash at the key
* `HDEL key field [field ...]` - Deletes the fields present inside the hash at the key
* `HEXISTS key field` - Returns a true or false based on field is present in hash at key or not
* `HKEYS key` - Returns all field present in the hash at key
* `HVALS key` - Returns all values present in the hash at key
* `FLUSHALL` - Deletes all keys
* `EXPIRE key seconds` - Expire key after given seconds
* `TTL key` - Returns the time in seconds remaining before key expires. -1 if key has no expiry, -2 if key is not present.

## Run 

To run server run the following command on repository root

```text
export TREDS_PORT=7997
go run main.go -port 7997
```

Using docker
```text
docker run -p 7997:7997 absolutelightning/treds
```

For CLI run the following command in the `client` folder in the repo

```text
cd ./client
export TREDS_PORT=7997
go run main.go -port 7997
```

`Default Port of Treds is 7997`
`If port is set in env variable as well as flag, flag takes the precedence.`

## Generating Binaries

To build the binary for the treds server, run following command in repo root - 
Binary named `treds` will be generated in repo root.

```text
make build             
```
```text
GOOS=linux GOARCH=arm64 make build
```

To build the binary for the treds cli, run following command in repo root -
Binary named `treds-cli` will be generated in repo root.

```text
make build-cli             
```
```text
GOOS=linux GOARCH=arm64 make build-cli
```


## Future Work
* Add Raft for HA and Persistence
* Transactions
* Pipelines
* More Commands ...
