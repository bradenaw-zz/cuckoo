package cuckoo

import (
	"encoding/hex"
	"math/rand"
	"testing"

	"github.com/bradenaw/trand"
	"github.com/stretchr/testify/require"
)

func TestBucketSort(t *testing.T) {
	b := bucket{l: 4, entries: [8]fingerprint{0xF, 0x0, 0x1, 0xA}}
	b.sort()
	require.Equal(t, bucket{l: 4, entries: [8]fingerprint{0x0, 0x1, 0xA, 0xF}}, b)

	b = bucket{l: 4, entries: [8]fingerprint{0x2C, 0x0F, 0x35, 0x1A}}
	b.sort()
	require.Equal(t, bucket{l: 4, entries: [8]fingerprint{0x0F, 0x1A, 0x2C, 0x35}}, b)
}

func TestBucketEncode(t *testing.T) {
	check := func(enc bucketEncoding, b bucket) {
		bits := enc.encode(b)
		b2 := enc.decode(bits)
		b.sort()
		b2.sort()
		require.Equal(t, b, b2)
	}

	check(packedBucketEncoding{f: 4}, bucket{l: 4, entries: [8]fingerprint{0x0, 0x0, 0x0, 0x0}})
	check(packedBucketEncoding{f: 4}, bucket{l: 4, entries: [8]fingerprint{0xA, 0x0, 0x0, 0x0}})
	check(packedBucketEncoding{f: 4}, bucket{l: 4, entries: [8]fingerprint{0x0, 0xF, 0x1, 0xA}})
	check(packedBucketEncoding{f: 5}, bucket{l: 4, entries: [8]fingerprint{0x1C, 0x0F, 0x15, 0x1A}})
	check(packedBucketEncoding{f: 6}, bucket{l: 4, entries: [8]fingerprint{0x2C, 0x0F, 0x35, 0x1A}})
	check(packedBucketEncoding{f: 8}, bucket{l: 4, entries: [8]fingerprint{0x8C, 0x7D, 0x38, 0x44}})

	check(directBucketEncoding{f: 2, b: 4}, bucket{l: 4, entries: [8]fingerprint{0x0, 0x0, 0x0, 0x0}})
	check(directBucketEncoding{f: 2, b: 4}, bucket{l: 4, entries: [8]fingerprint{0x3, 0x0, 0x0, 0x0}})
	check(directBucketEncoding{f: 2, b: 4}, bucket{l: 4, entries: [8]fingerprint{0x0, 0x3, 0x1, 0x2}})
	check(directBucketEncoding{f: 4, b: 4}, bucket{l: 4, entries: [8]fingerprint{0x0, 0x0, 0x0, 0x0}})
	check(directBucketEncoding{f: 4, b: 4}, bucket{l: 4, entries: [8]fingerprint{0xA, 0x0, 0x0, 0x0}})
	check(directBucketEncoding{f: 4, b: 4}, bucket{l: 4, entries: [8]fingerprint{0x0, 0xF, 0x1, 0xA}})
	check(directBucketEncoding{f: 5, b: 4}, bucket{l: 4, entries: [8]fingerprint{0x1C, 0x0F, 0x15, 0x1A}})
	check(directBucketEncoding{f: 6, b: 4}, bucket{l: 4, entries: [8]fingerprint{0x2C, 0x0F, 0x35, 0x1A}})
	check(directBucketEncoding{f: 8, b: 4}, bucket{l: 4, entries: [8]fingerprint{0x8C, 0x7D, 0x38, 0x44}})
}

func TestBasic(t *testing.T) {
	f := NewRaw(4, 4, 7)
	key := []byte{0x51}
	require.Equal(t, f.Contains(key), No)
	f.Add(key)
	require.Equal(t, f.Contains(key), Maybe)

	f.Add([]byte{0x77})
	require.Equal(t, f.Contains(key), Maybe)

	f.Add([]byte{0x19, 0x39})
	require.Equal(t, f.Contains(key), Maybe)
}

func TestManyWithFP(t *testing.T) {
	trand.RandomN(t, 100, func(t *testing.T, r *rand.Rand) {
		testRandomWithFP(t, r, r.Int()%2000+10)
	})
}

func TestManyRaw(t *testing.T) {
	trand.RandomN(t, 100, func(t *testing.T, r *rand.Rand) {
		testRandomWithRaw(t, r, r.Int()%2000+10)
	})
}

func TestLargeWithFP(t *testing.T) {
	trand.RandomN(t, 5, func(t *testing.T, r *rand.Rand) {
		testRandomWithFP(t, r, r.Int()%1000000+100000)
	})
}

func TestLargeRaw(t *testing.T) {
	trand.RandomN(t, 5, func(t *testing.T, r *rand.Rand) {
		testRandomWithRaw(t, r, r.Int()%1000000+100000)
	})
}

func testRandomWithFP(t *testing.T, r *rand.Rand, n int) {
	fp := rand.Float64()*0.10 + 0.01

	fl := New(n, fp)

	testRandom(t, r, n, fl)

	numFP := 0
	for i := 0; i < n; i++ {
		var key [8]byte
		_, _ = r.Read(key[:])
		if fl.Contains(key[:]) == Maybe {
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

func testRandomWithRaw(t *testing.T, r *rand.Rand, n int) {
	f := r.Int()%14 + 3
	b := r.Int()%8 + 1
	if f*b > 64 {
		b = 4
	}

	fl := NewRaw(f, b, n)

	testRandom(t, r, n, fl)
}

func testRandom(t *testing.T, r *rand.Rand, n int, fl *Filter) {
	items := make([][]byte, n)
	for i := range items {
		var key [8]byte
		_, _ = r.Read(key[:])
		items[i] = key[:]
		fl.Add(items[i])
		require.Equal(t, Maybe, fl.Contains(items[i]), "item %d broken", i)
	}

	fl.check()

	for i := range items {
		require.Equal(t, Maybe, fl.Contains(items[i]), "item %s missing", hex.EncodeToString(items[i]))
	}
}
