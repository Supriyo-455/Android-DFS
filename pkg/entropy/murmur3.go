package entropy

import (
	"encoding/binary"
	"math/bits"
)

const (
	c1_32 uint32 = 0xcc9e2d51
	c2_32 uint32 = 0x1b873593
)

// Murmur3_32 computes the 32-bit MurmurHash3 value with a given seed.
// Murmur3 is extremely fast and ideal for Merkle tree node aggregation.
func Murmur3_32(data []byte, seed uint32) uint32 {
	h1 := seed
	nblocks := len(data) / 4

	for i := 0; i < nblocks; i++ {
		k1 := binary.LittleEndian.Uint32(data[i*4:])

		k1 *= c1_32
		k1 = bits.RotateLeft32(k1, 15)
		k1 *= c2_32

		h1 ^= k1
		h1 = bits.RotateLeft32(h1, 13)
		h1 = h1*5 + 0xe6546b64
	}

	tail := data[nblocks*4:]
	var k1 uint32

	switch len(tail) {
	case 3:
		k1 ^= uint32(tail[2]) << 16
		fallthrough
	case 2:
		k1 ^= uint32(tail[1]) << 8
		fallthrough
	case 1:
		k1 ^= uint32(tail[0])
		k1 *= c1_32
		k1 = bits.RotateLeft32(k1, 15)
		k1 *= c2_32
		h1 ^= k1
	}

	// Finalization mix
	h1 ^= uint32(len(data))
	h1 ^= h1 >> 16
	h1 *= 0x85ebca6b
	h1 ^= h1 >> 13
	h1 *= 0xc2b2ae35
	h1 ^= h1 >> 16

	return h1
}

// Murmur3_64 computes a 64-bit hash using Murmur3 principles for larger key spaces.
func Murmur3_64(data []byte, seed uint64) uint64 {
	h32_1 := Murmur3_32(data, uint32(seed))
	h32_2 := Murmur3_32(data, uint32(seed>>32)^0x5bd1e995)
	return (uint64(h32_1) << 32) | uint64(h32_2)
}
