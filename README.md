# Cuckoo

[![GoDoc](https://godoc.org/github.com/bradenaw/cuckoo?status.svg)](https://godoc.org/github.com/bradenaw/cuckoo)

Package cuckoo implements a cuckoo filter using the techniques described in
https://www.cs.cmu.edu/~dga/papers/cuckoo-conext2014.pdf.

Cuckoo filters are space-efficient, probabalistic structures for set membership tests.  Essentially,
a Cuckoo filter behaves like a set, but the only query it supports is "is x a member of the set?",
to which it can only respond "no" or "maybe".

This is useful for many purposes, but one use case is avoiding expensive lookups. A Cuckoo filter of
the items contained in the store is capable of definitively saying that an item is not in the store,
and a lookup can be skipped entirely.

The rate at which the filter responds "maybe" when an item wasn't actually added to the filter is
configurable, and changes the space used by the filter. If too many items are added to a filter, it
overflows and returns "maybe" for every query, becoming useless.

Cuckoo filters are similar to Bloom filters, a more well-known variety of set-membership filter.
However, Cuckoo filters have two main advantages:

- For false positive rates below about 3%, Cuckoo filters use less space than a corresponding Bloom
  filter.

- Cuckoo filters support Delete(), and Bloom filters do not.
