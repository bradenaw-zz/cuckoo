package cuckoo

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/bradenaw/trand"
)


func TestBucketSort(t *testing.T) {
	b := bucket{0xF, 0x0, 0x1, 0xA}
	b.sort()
	require.Equal(t, bucket{0x0, 0x1, 0xA, 0xF}, b)
}

func TestBucketEncode(t *testing.T) {
	b := bucket{0x0, 0xF, 0x1, 0xA}
	bits := b.encode()
	var b2 bucket
	b2.decode(bits)
	require.Equal(t, b, b2)
}

func TestBasic(t *testing.T) {
	f := New(20, 0.01)
	key := []byte{0x51}
	require.Equal(t, f.Contains(key), No)
	f.Add(key)
	require.Equal(t, f.Contains(key), Maybe)
}

func TestMany(t *testing.T) {
	trand.RandomN(t, 100, func(t *testing.T, r *rand.Rand) {
		testRandom(t, r, r.Int()%50+50)
	})
}

func TestHuge(t *testing.T) {
	trand.RandomN(t, 1, func(t *testing.T, r *rand.Rand) {
		testRandom(t, r, r.Int()%1000000+100000)
	})
}

func testRandom(t *testing.T, r *rand.Rand, n int) {
	fp := r.Float64()*0.02 + 0.01
	f := New(n, fp)

	items := make([][]byte, n)
	for i := range items {
		var key [8]byte
		_, _ = r.Read(key[:])
		items[i] = key[:]
		f.Add(items[i])
		require.Equal(t, Maybe, f.Contains(items[i]), "item %d broken", i)
	}

	for i := range items {
		require.Equal(t, Maybe, f.Contains(items[i]))
	}

	numFP := 0
	for i := 0; i < n; i++ {
		var key [8]byte
		_, _ = r.Read(key[:])
		if f.Contains(key[:]) == Maybe {
			numFP++
		}
	}
	require.True(
		t,
		numFP < int(float64(n)*fp*2)+4,
		"%d false positives in %d attempts (%f%%)",
		numFP, n, float64(numFP)/float64(n)*100,
	)
}
