package bloom

import (
	"math"
	"testing"
	"time"

	"encoding/binary"

	"github.com/alicebob/miniredis"
)

type TestClient struct {
	mem map[string][]int
}

func NewTestClient() Client {
	return &TestClient{
		mem: map[string][]int{},
	}
}

func (r *TestClient) Set(key string, offset uint, value int) error {
	if r.mem[key] == nil {
		r.mem[key] = make([]int, math.MaxInt16)
	}
	r.mem[key][offset] = value
	return nil
}

func (r *TestClient) Get(key string, offset uint) (int, error) {
	if r.mem[key] == nil {
		return 0, nil
	}
	return r.mem[key][offset], nil
}

func (r *TestClient) Delete(keys ...string) error {
	for _, key := range keys {
		delete(r.mem, key)
	}
	return nil
}

func (r *TestClient) Expire(key string, duration time.Duration) error {
	return nil
}

func TestRedisBloomFilter(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		t.Error("Miniredis could not start")
	}
	defer s.Close()

	m, k := EstimateParameters(1000, .01)
	bitSet := NewRedisBitSet("test_key", m, NewTestClient())
	b := New(m, k, bitSet)
	testBloomFilter(t, b)
}

func TestBloomFilter(t *testing.T) {
	m, k := EstimateParameters(1000, .01)
	b := New(m, k, NewBitSet(m))
	testBloomFilter(t, b)
}

func TestCollision(t *testing.T) {
	n := uint(10000)
	fp := .01
	m, k := EstimateParameters(n, fp)
	b := New(m, k, NewBitSet(m))
	shouldNotExist := 0
	for i := uint(0); i < n; i++ {
		data := make([]byte, 4)
		binary.LittleEndian.PutUint32(data, uint32(i))
		existsBefore, err := b.Exists(data)
		if err != nil {
			t.Fatal("Error checking existence.")
		}
		if existsBefore {
			shouldNotExist = shouldNotExist + 1
		}
		err = b.Add(data)
		if err != nil {
			t.Fatal("Error adding item.")
		}
		existsAfter, err := b.Exists(data)
		if err != nil {
			t.Fatal("Error checking existence.")
		}
		if !existsAfter {
			t.Fatal("Item should exist.")
		}
	}
	if float64(shouldNotExist) > fp*float64(n) {
		t.Fatal("Too many false positives.")
	}
}

func testBloomFilter(t *testing.T, b *BloomFilter) {
	data := []byte("some key")
	existsBefore, err := b.Exists(data)
	if err != nil {
		t.Fatal("Error checking for existence in bloom filter")
	}
	if existsBefore {
		t.Fatal("Bloom filter should not contain this data")
	}
	err = b.Add(data)
	if err != nil {
		t.Fatal("Error adding to bloom filter")
	}
	existsAfter, err := b.Exists(data)
	if err != nil {
		t.Fatal("Error checking for existence in bloom filter")
	}
	if !existsAfter {
		t.Fatal("Bloom filter should contain this data")
	}
}
