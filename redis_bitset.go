package bloom

import (
	"fmt"
	"time"
)

const redisMaxLength = 512 * 1024 * 1024

type Client interface {
	Set(key string, offset uint, value int) error
	Get(key string, offset uint) (int, error)
	Delete(keys ...string) error
	Expire(key string, duration time.Duration) error
}

type RedisBitSet struct {
	keyPrefix string
	cli       Client
	m         uint
}

func NewRedisBitSet(keyPrefix string, m uint, conn Client) *RedisBitSet {
	return &RedisBitSet{keyPrefix, conn, m}
}

func (r *RedisBitSet) Set(offsets []uint) error {
	for _, offset := range offsets {
		key, thisOffset := r.getKeyOffset(offset)
		err := r.cli.Set(key, thisOffset, 1)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *RedisBitSet) Test(offsets []uint) (bool, error) {
	for _, offset := range offsets {
		key, thisOffset := r.getKeyOffset(offset)
		bitValue, err := r.cli.Get(key, thisOffset)
		if err != nil {
			return false, err
		}
		if bitValue == 0 {
			return false, nil
		}
	}

	return true, nil
}

func (r *RedisBitSet) Expire(duration time.Duration) error {
	n := uint(0)
	for n <= uint(r.m/redisMaxLength) {
		key := fmt.Sprintf("%s:%d", r.keyPrefix, n)
		n = n + 1
		err := r.cli.Expire(key, duration)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *RedisBitSet) Delete() error {
	n := uint(0)
	keys := make([]string, 0)
	for n <= uint(r.m/redisMaxLength) {
		key := fmt.Sprintf("%s:%d", r.keyPrefix, n)
		keys = append(keys, key)
		n = n + 1
	}
	return r.cli.Delete(keys...)
}

func (r *RedisBitSet) getKeyOffset(offset uint) (string, uint) {
	n := uint(offset / redisMaxLength)
	thisOffset := offset - n*redisMaxLength
	key := fmt.Sprintf("%s:%d", r.keyPrefix, n)
	return key, thisOffset
}
