package bloom

import (
	"testing"
	"time"
)

func TestRedisBitSet_New_Set_Test(t *testing.T) {

	bitSet := NewRedisBitSet("test_key", 512, NewTestClient())
	isSetBefore, err := bitSet.Test([]uint{0})
	if err != nil {
		t.Error("Could not test bitset in redis")
	}
	if isSetBefore {
		t.Error("Bit should not be set")
	}
	err = bitSet.Set([]uint{512})
	if err != nil {
		t.Error("Could not set bitset in redis")
	}
	isSetAfter, err := bitSet.Test([]uint{512})
	if err != nil {
		t.Error("Could not test bitset in redis")
	}
	if !isSetAfter {
		t.Error("Bit should be set")
	}
	err = bitSet.Expire(3600 * time.Second)
	if err != nil {
		t.Errorf("Error adding expiration to bitset: %v", err)
	}
	err = bitSet.Delete()
	if err != nil {
		t.Errorf("Error cleaning up bitset: %v", err)
	}
}
