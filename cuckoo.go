// Package cuckoo implements a cuckoo filter using the techniques described in
// https://www.cs.cmu.edu/~dga/papers/cuckoo-conext2014.pdf.
//
// Cuckoo filters are space-efficient, probabalistic structures for set membership tests.
// Essentially, a Cuckoo filter behaves like a set, but the only query it supports is "is x a member
// of the set?", to which it can only respond "no" or "maybe".
//
// This is useful for many purposes, but one use case is avoiding expensive lookups. A Cuckoo filter
// of the items contained in the store is capable of definitively saying that an item is not in the
// store, and a lookup can be skipped entirely.
//
// The rate at which the filter responds "maybe" when an item wasn't actually added to the filter is
// configurable, and changes the space used by the filter.
//
// Cuckoo filters are similar to Bloom filters, a more well-known variety of set-membership filter.
// However, Cuckoo filters have two main advantages:
//
// - For false positive rates below about 3%, Cuckoo filters use less space than a corresponding
// Bloom filter.
//
// - Cuckoo filters support Delete(), and Bloom filters do not.
package cuckoo

import (
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"math"
	"math/bits"
	"math/rand"
	"strings"

	"github.com/bradenaw/bitarray"
)

type Filter struct {
	inner bitarray.BitArray
	count int
	full  bool
}

type Result byte

const (
	// The item is definitely not a member of the set.
	No Result = iota
	// The item might be a member of the set.
	Maybe
)

// Returns a new filter capable of holding n items with an estimated false-positive rate of fp.
// If more than n items are added, the false-positive rate approaches 1.
func New(n int, fp float64) *Filter {
	loadFactor := 0.95
	if n > 1<<20 {
		loadFactor = 0.6
	}
	if n > 1<<25 {
		loadFactor = 0.4
	}
	bitsPerItem := (math.Log2(1/fp) + 2) / loadFactor
	// Round numBuckets to an even power of two, so that the xor operations work.
	numBuckets := 1 << uint(64-bits.LeadingZeros64(uint64(float64(n)*bitsPerItem)))
	return &Filter{
		inner: bitarray.New(numBuckets, 12),
	}
}

// Returns the number of bytes used by the filter.
func (fl *Filter) SizeBytes() uint64 {
	return uint64(fl.inner.Len()) * uint64(fl.inner.K())
}

func (fl *Filter) nBuckets() uint64 {
	return uint64(fl.inner.Len())
}

// Adds an item to the filter. After Add(x) returns, Contains(x) returns Maybe.
func (fl *Filter) Add(x []byte) {
	f, i1, i2 := fl.itemToIdxs(x)
	fl.count++

	// First, attempt to add x's fingerprint to either of its candidate buckets, as long as there's
	// room.
	is := [2]uint64{i1, i2}
	for _, i := range is {
		b := fl.getBucket(i)
		if b.hasEmpty() {
			b.add(f)
			fl.setBucket(i, b)
			return
		}
	}

	// If there isn't any room, then we have to kick something out of one of the buckets (placing it
	// in its other candidate bucket) in order to make room.
	i := is[rand.Int()%len(is)]
	maxNumKicks := 500
	b := fl.getBucket(i)
	for n := 0; n < maxNumKicks; n++ {
		entry := rand.Int() % 4
		f, b[entry] = b[entry], f
		fl.setBucket(i, b)
		i = fl.otherIdx(f, i)
		b = fl.getBucket(i)
		if b.hasEmpty() {
			b.add(f)
			fl.setBucket(i, b)
			return
		}
		// But if there's no room in the bucket we're kicking to, then we have to kick something out
		// of _that_ bucket, so loop around again.
	}
	// If we made it here, then we did maxNumKicks successive kicks without finding a bucket with
	// empty space, so we should just consider the filter 'full' and return Maybe for everything
	// from now on.
	fl.full = true
}

// Deletes x from the filter. x must have been previously added.
func (fl *Filter) Delete(x []byte) {
	fl.count--
	f, i1, i2 := fl.itemToIdxs(x)

	is := [2]uint64{i1, i2}
	for _, i := range is {
		b := fl.getBucket(i)
		if b.contains(f) {
			b.delete(f)
			fl.setBucket(i, b)
			return
		}
	}
	panic(fmt.Sprintf("item %s not previously inserted", hex.EncodeToString(x)))
}

// Returns No if x is definitely not in the filter, and Maybe if x might be in the filter.
func (fl *Filter) Contains(x []byte) Result {
	if fl.full {
		return Maybe
	}
	f, i1, i2 := fl.itemToIdxs(x)
	is := [2]uint64{i1, i2}
	for _, i := range is {
		b := fl.getBucket(i)
		if b.contains(f) {
			return Maybe
		}
	}
	return No
}

func (fl *Filter) Count() int {
	return fl.count
}

// Given x, returns x's fingerprint and the indexes of the two buckets that x's fingerprint would be
// placed in.
func (fl *Filter) itemToIdxs(x []byte) (fingerprint, uint64, uint64) {
	h := fl.hashItem(x)
	f := fl.hashToFingerprint(h)
	i1 := h % fl.nBuckets()
	return f, i1, fl.otherIdx(f, i1)
}

// Given either index that fingerprint would be contained in, returns the other one.
func (fl *Filter) otherIdx(f fingerprint, i1 uint64) uint64 {
	return (i1 ^ fl.hashFingerprint(f)) % fl.nBuckets()
}

func (fl *Filter) getBucket(i uint64) bucket {
	var b bucket
	b.decode(uint16(fl.inner.Get(int(i))))
	return b
}

func (fl *Filter) setBucket(i uint64, b bucket) {
	bits := b.encode()
	fl.inner.Set(int(i), uint64(bits))
}

func (fl *Filter) hashToFingerprint(hash uint64) fingerprint {
	// Prefer the high bits of the hash, because the low bits are used for i1.
	for shift := uint(60); shift > 0; shift -= 4 {
		result := fingerprint((hash >> shift) & 0xF)
		if result != 0 {
			return result
		}
	}
	return 1
}

func (fl *Filter) hashItem(x []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(x)
	return h.Sum64()
}

func (fl *Filter) hashFingerprint(x fingerprint) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte{byte(x)})
	return h.Sum64()
}

// A 4-bit fingerprint of an element. 0 means 'none'.
type fingerprint byte
type bucket [4]fingerprint

// Returns true if this bucket contains the given fingerprint.
func (b bucket) contains(f fingerprint) bool {
	for _, entry := range b {
		if entry == f {
			return true
		}
	}
	return false
}

// Add one instance of the given fingerprint to this bucket.
func (b *bucket) add(f fingerprint) {
	for i := range b {
		if i == 0 {
			b[i] = f
		}
	}
}

// Deletes one instance of the given fingerprint from this bucket.
func (b *bucket) delete(f fingerprint) {
	for i, f2 := range b {
		if f == f2 {
			b[i] = 0
		}
	}
}

// Returns true if this bucket has an empty slot.
func (b *bucket) hasEmpty() bool {
	for i := range b {
		if b[i] == 0 {
			return true
		}
	}
	return false
}

// Returns a 12-bit encoding of the bucket.
func (b *bucket) encode() uint16 {
	// sort, while mutating bucket, isn't destructive because the order of items isn't meaningful
	b.sort()
	// 4 fingerprints of 4 bits each fit into 16 bits.
	packed := (uint16(b[0]) << 12) |
		(uint16(b[1]) << 8) |
		(uint16(b[2]) << 4) |
		uint16(b[3])
	// But because the order doesn't matter, there are only 3,876 possible buckets, which is
	// encodable in 12 bits.
	return lookupFingerprintsToBits[packed]
}

// Inverse of encode().
func (b *bucket) decode(x uint16) {
	packed := lookupBitsToFingerprints[x]
	b[0] = fingerprint(packed >> 12)
	b[1] = fingerprint((packed >> 8) & 0xf)
	b[2] = fingerprint((packed >> 4) & 0xf)
	b[3] = fingerprint((packed) & 0xf)
}

func (b *bucket) sort() {
	for i := 0; i < len(b); i++ {
		for j := i + 1; j < len(b); j++ {
			if b[j-1] > b[j] {
				b[j-1], b[j] = b[j], b[j-1]
			}
		}
	}
}

func (b bucket) String() string {
	s := make([]string, len(b))
	for i := range b {
		s[i] = fmt.Sprintf("%x", b[i])
	}
	return strings.Join(s, ",")
}
