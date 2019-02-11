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
// configurable, and changes the space used by the filter. If too many items are added to a filter,
// it overflows and returns "maybe" for every query, becoming useless.
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
	// The bitarray used to store buckets encoded with bucketEncoding.
	inner          bitarray.Array
	bucketEncoding bucketEncoding
	// The number of items in the filter.
	count int
	// True if the filter is overflowed, and now just returns Maybe for all queries.
	overflowed bool
	// The number of bits per fingerprint.
	f int
}

type Result byte

const (
	// The item is definitely not a member of the set.
	No Result = iota
	// The item might be a member of the set.
	Maybe
)

func (r Result) String() string {
	switch r {
	case No:
		return "No"
	case Maybe:
		return "Maybe"
	default:
		return fmt.Sprintf("Unknown(%d)", r)
	}
}

// Returns a new filter capable of holding n items with an estimated false-positive rate of fp.
// If more than n items are added, the false-positive rate approaches 1.
func New(n int, fp float64) *Filter {
	b := 4
	f := int(math.Min(math.Max(math.Ceil(math.Log2(2*float64(b)/float64(fp))), 4), 16))
	loadFactor := 0.95
	return NewRaw(f, b, int(float64(n)/float64(b)/float64(loadFactor)))
}

// Returns a new filter constructed using raw parameters.
//
// - f: fingerprint length in bits. [2, 16]
//
// - b: bucket size in number of entries. [1, 8]
//
// - n: number of buckets in the table.
//
// f * b must be less than 64.
//
// The most efficient representation can be used when f=4 and b=4, using the fewest bits per item.
//
// See https://www.cs.cmu.edu/~dga/papers/cuckoo-conext2014.pdf for more information on how to
// select these parameters.
func NewRaw(f, b, n int) *Filter {
	if f < 2 || f > 16 || b < 1 || b > 8 || f*b > 64 {
		panic("invalid params")
	}

	// Round n to an even power of two, so that the xor operations work.
	n = 1 << uint(bits.Len64(uint64(n)))
	var enc bucketEncoding
	if f >= 4 && b == 4 {
		enc = packedBucketEncoding{f}
	} else {
		enc = directBucketEncoding{f, b}
	}
	return &Filter{
		inner:          bitarray.New(n, uint(enc.size())),
		f:              f,
		bucketEncoding: enc,
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
	fl.count++
	if fl.overflowed {
		return
	}
	f, i1, i2 := fl.itemToIdxs(x)

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
		entry := rand.Int() % b.l
		f, b.entries[entry] = b.entries[entry], f
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
	// empty space, so we should just consider the filter 'overflowed' and return Maybe for
	// everything from now on.
	fl.overflowed = true
}

// Deletes x from the filter. x must have been previously added.
func (fl *Filter) Delete(x []byte) {
	fl.count--
	if fl.overflowed {
		return
	}
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
	if fl.overflowed {
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

// True if the filter has overflowed, and now blindly returns Maybe for every query. This happens
// when an Add() fails because there is no more room left in the filter.
func (fl *Filter) Overflowed() bool {
	return fl.overflowed
}

// Returns the number of items in the filter.
func (fl *Filter) Count() int {
	return fl.count
}

func (fl *Filter) dump() {
	for i := uint64(0); i < fl.nBuckets(); i++ {
		b := fl.getBucket(i)
		fmt.Printf("%d: %08x %s\n", i, fl.inner.Get(int(i)), b)
	}
}

func (fl *Filter) check() {
	for i := uint64(0); i < fl.nBuckets(); i++ {
		b := fl.getBucket(i)
		bits := fl.inner.Get(int(i))
		if bits != fl.bucketEncoding.encode(b) {
			panic("bucket broken")
		}
	}
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
	return fl.bucketEncoding.decode(fl.inner.Get(int(i)))
}

func (fl *Filter) setBucket(i uint64, b bucket) {
	bits := fl.bucketEncoding.encode(b)
	fl.inner.Set(int(i), bits)
}

func (fl *Filter) hashToFingerprint(hash uint64) fingerprint {
	// Prefer the high bits of the hash, because the low bits are used for i1.
	mask := (uint64(1) << uint(fl.f)) - 1
	for shift := 64 - fl.f; shift > 0; shift -= fl.f {
		result := fingerprint((hash >> uint(shift)) & mask)
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
	_, _ = h.Write([]byte{byte(x >> 8), byte(x)})
	return h.Sum64()
}

type bucketEncoding interface {
	encode(b bucket) uint64
	decode(x uint64) bucket
	size() uint64
}

// Direct encoding of buckets. Just appends each of the fingerprints to each other to make a
// (f*b)-bit encoding.
type directBucketEncoding struct {
	f int
	b int
}

func (e directBucketEncoding) encode(b bucket) uint64 {
	result := uint64(0)
	for i := 0; i < e.b; i++ {
		result |= uint64(b.entries[i]) << uint(i*e.f)
	}
	return result
}
func (e directBucketEncoding) decode(x uint64) bucket {
	var result bucket
	result.l = e.b
	mask := (uint64(1) << uint(e.f)) - 1
	for i := 0; i < e.b; i++ {
		result.entries[i] = fingerprint((x >> uint(i*e.f)) & mask)
	}
	return result
}
func (e directBucketEncoding) size() uint64 {
	return uint64(e.f * e.b)
}

// Packed encoding of buckets.
//
// Uses the technique from https://www.cs.cmu.edu/~dga/papers/cuckoo-conext2014.pdf section 5.2 to
// save one bit per fingerprint in buckets of size 4.
//
// As such, only works with buckets of size 4 and fingerprints of size >=4.
type packedBucketEncoding struct{ f int }

func (e packedBucketEncoding) encode(b bucket) uint64 {
	// The order of items isn't meaningful. And because the order doesn't matter, there are only
	// 3,876 possible buckets, which is encodable in 12 bits.
	e.sortBucketByLower4(&b)

	result := uint64(0)
	packed := uint16(0)
	for i := 0; i < 4; i++ {
		packed |= uint16(b.entries[i]&0xF) << uint(12-4*i)
		result |= (uint64(b.entries[i]) >> 4) << uint((e.f-4)*i)
	}
	size := e.size()
	result |= uint64(lookupFingerprintsToBits[packed]) << uint(size-12)
	return result
}

func (e packedBucketEncoding) decode(x uint64) bucket {
	size := e.size()
	var b bucket
	b.l = 4
	packed := lookupBitsToFingerprints[uint16(x>>uint(size-12))]
	mask := (uint64(1) << uint(e.f-4)) - 1
	for i := 0; i < 4; i++ {
		b.entries[i] = fingerprint((uint64(packed) >> uint(12-4*i)) & 0xf)
		b.entries[i] |= fingerprint((x >> uint((e.f-4)*i) & mask) << 4)
	}
	return b
}

func (e packedBucketEncoding) size() uint64 {
	return uint64(12 + (e.f-4)*4)
}

func (e packedBucketEncoding) sortBucketByLower4(b *bucket) {
	for i := 3; i >= 0; i-- {
		for j := 0; j < i; j++ {
			if (b.entries[j] & 0xF) > (b.entries[j+1] & 0xF) {
				b.entries[j], b.entries[j+1] = b.entries[j+1], b.entries[j]
			}
		}
	}
}

// A fingerprint of an element. 0 means 'none'.
type fingerprint uint16
type bucket struct {
	// This looks a lot like a slice but doing it this way means no allocations needed.
	entries [8]fingerprint
	l       int // The length of `entries`.
}

// Returns true if this bucket contains the given fingerprint.
func (b bucket) contains(f fingerprint) bool {
	for i := 0; i < b.l; i++ {
		if b.entries[i] == f {
			return true
		}
	}
	return false
}

// Add one instance of the given fingerprint to this bucket.
func (b *bucket) add(f fingerprint) {
	for i := 0; i < b.l; i++ {
		if b.entries[i] == 0 {
			b.entries[i] = f
			return
		}
	}
}

// Deletes one instance of the given fingerprint from this bucket.
func (b *bucket) delete(f fingerprint) {
	for i := 0; i < b.l; i++ {
		if f == b.entries[i] {
			b.entries[i] = 0
		}
	}
}

// Returns true if this bucket has an empty slot.
func (b *bucket) hasEmpty() bool {
	for i := 0; i < b.l; i++ {
		if b.entries[i] == 0 {
			return true
		}
	}
	return false
}

func (b *bucket) sort() {
	for i := b.l - 1; i >= 0; i-- {
		for j := 0; j < i; j++ {
			if b.entries[j] > b.entries[j+1] {
				b.entries[j], b.entries[j+1] = b.entries[j+1], b.entries[j]
			}
		}
	}
}

func (b bucket) String() string {
	s := make([]string, b.l)
	for i := 0; i < b.l; i++ {
		s[i] = fmt.Sprintf("%x", b.entries[i])
	}
	return strings.Join(s, ",")
}
