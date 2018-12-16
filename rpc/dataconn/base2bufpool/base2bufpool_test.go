package base2bufpool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPoolWorksBelowMinSize(t *testing.T) {
	pool := New(15, 20)
	buf := pool.Get(1 << 14)
	assert.Equal(t, uint(1<<14), buf.payloadLen)
}

func TestPoolWorksAboveMaxSize(t *testing.T) {
	pool := New(15, 20)
	buf := pool.Get(1 << 22)
	assert.Equal(t, uint(1<<22), buf.payloadLen)
}

func TestPoolRoundsUp(t *testing.T) {
	pool := New(15, 20)
	buf := pool.Get((1 << 15) + 23)
	assert.Equal(t, 1<<16, len(buf.shiftBuf))
	assert.Equal(t, (1<<15) + 23, len(buf.Bytes()))
}


func TestPoolDoesNotRoundPowersOfTwo(t *testing.T) {
	pool := New(15,20)
	buf := pool.Get(1<<17)
	assert.Equal(t, 1<<17, len(buf.Bytes()))
}

func TestFittingSHift(t *testing.T) {
	assert.Equal(t, uint(16), fittingShift(1 + 1<<15))
	assert.Equal(t, uint(15), fittingShift(1<<15))
}

func TestFreeFromPoolRangeWorks(t *testing.T) {
	pool := New(15, 20)
	buf := pool.Get(1<<16)
	buf.Free()
}

func TestFreeFromOutOfPoolRangeWorks(t *testing.T) {
	pool := New(15, 20)
	buf := pool.Get(1<<23)
	buf.Free()
}

func TestZeroWOrks(t *testing.T) {
	pool := New(15, 20)
	buf := pool.Get(0)
	buf.Free()
}

