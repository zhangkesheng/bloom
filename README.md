# Bloom Filters for Golang

Bloom filter for go, backed by redis or in process bitset

If you are not familiar with how Bloom filters work and their usefulness, 
[please read](https://en.wikipedia.org/wiki/Bloom_filter).

[![Build Status](https://travis-ci.org/bculberson/svg?branch=master)](https://travis-ci.org/bculberson/bloom)

## Example Usage (in process):

install with 
```
go get gopkg.in/bculberson/v2
```

``go
import (
  "gopkg.in/bculberson/v2"
)
``

This bloom filter is initialized to hold 1000 keys and
will have a false positive rate of 1% (.01).

```go
m, k := EstimateParameters(1000, .01)
b := New(m, k, NewBitSet(m))
b.Add([]byte("some key"))
exists, _ := b.Exists([]byte("some key"))
doesNotExist, _ := b.Exists([]byte("some other key"))
```

## Example Usage (redis backed):

This bloom filter is initialized to hold 1000 keys and
will have a false positive rate of 1% (.01).

This library uses [http://github.com/garyburd/redigo/redis](http://github.com/garyburd/redigo/redis)

```go
pool := &redis.Pool{
    MaxIdle:     3,
    IdleTimeout: 240 * time.Second,
    Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", addr) },
}


conn := pool.Get()
m, k := EstimateParameters(1000, .01)
bitSet := NewRedisBitSet("test_key", m, conn)
b := New(m, k, bitSet)
b.Add([]byte("some key"))
exists, _ := b.Exists([]byte("some key"))
doesNotExist, _ := b.Exists([]byte("some other key"))
```
