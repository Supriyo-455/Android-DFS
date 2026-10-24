package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
)

// GearTable is a 256-element uint64 table used by the FastCDC rolling hash.
// This is the standard 64-bit Gear matrix introduced in the FastCDC research paper.
var GearTable = [256]uint64{
	0x4642b47424fb24e7, 0xef1be7308c9082ef, 0x72a2432924376c66, 0x228f44ff532d8495,
	0x07f16ef066df157e, 0x618ff060f64be872, 0x217aa0e2b47be293, 0x1d4d3a042e646270,
	0xc729052b61f95d8d, 0x6e2b0c3f5ea4e4bb, 0x90432f41b31527ef, 0x3f5c531d05417da5,
	0x2b380ef307a514d3, 0x5158a14b533d6b05, 0x2a912bb1476d0615, 0x67db20165972809f,
	0xd747b0a8848d5d9b, 0x765a0832043e753f, 0x3d0cf33c6f85d263, 0x48a8e104191c7849,
	0x7c7aa3a8e9860b29, 0xb80214a1a5b8109d, 0x6f9f3ef4e6584f2b, 0x56a6a0e6631e8a8b,
	0x3b8d4f40f09b552b, 0x5e79603a110a12e3, 0x599b50b5711b7dfb, 0x3f4625b161df55db,
	0x475e1a0b345b17bb, 0x2e4250228d77c223, 0x6f5c814b732551bf, 0x4a946b0a61427fb3,
	0xa9d2872322409f53, 0x90ef14896e7368fb, 0x1cfc153a81285223, 0x82e2050f24956383,
	0xa41c7b89710e28f3, 0x4d59a722880b91e7, 0x05f52bb2131923db, 0x5c7f8936052843ef,
	0x1116c476046e7f8b, 0x16b60e68940b54f3, 0x6167885b546554b7, 0x3e18a99525418b77,
	0x546b38c20573e8df, 0x228020a568285513, 0x7460117a0225d7ef, 0x1737e126154b5df3,
	0x5e56e02f232778bb, 0x0c920c741498144b, 0x2774a98444a72d13, 0x5776a392815049fb,
	0x06060c2d33454afb, 0x7856e7922247b9fb, 0x67396655193910eb, 0x23a6d71b34384bf7,
	0x2474163914a844cb, 0x6717a6a42125749f, 0x3c233215160867df, 0x0b8a2e2264627447,
	0x59a44415256e4093, 0x3d0b2f56110f5afb, 0x7166164506828bbf, 0x3c3c734b121946eb,
	0x5d9b626e245a4407, 0x7f4b882315053db3, 0x442c5251644722eb, 0x70529e74246874eb,
	0x464522a454867493, 0x231846b41215444b, 0x37648356191b72e7, 0x6331a98606412163,
	0x2649b567220268fb, 0x7b615694112e52bb, 0x515646b5247321eb, 0x334468241416738b,
	0x616a695b214a1a5b, 0x5826629202511477, 0x36484e55122e23cb, 0x36342894172566db,
	0x2a3d7a860714150b, 0x41656b23115456f3, 0x7232231b144865e7, 0x2955554611594247,
	0x485121b6194b150f, 0x3c3b5324151b66df, 0x165b4412053912ef, 0x34545225134764b3,
	0x24245645163428fb, 0x35652515124b61fb, 0x2567243916297313, 0x61546255141525bf,
	0x54664875151522eb, 0x29497914041634ab, 0x1928495514292377, 0x56784535122718fb,
	0x67623945112521e3, 0x5464192b115474ab, 0x38495815154331bb, 0x164b5455124b22fb,
	0x2767225114161747, 0x5464522b1328456f, 0x17485926115413ef, 0x47562855142831bb,
	0x38495724124953bf, 0x26496814141652f3, 0x15495515114a159b, 0x6475521b143714b7,
	0x37495535123912fb, 0x47562945142516bb, 0x26496615112843ef, 0x57642815144b25ab,
	0x16495525121715fb, 0x48562745132924bf, 0x37496615141635f3, 0x26495525112817ab,
	0x58642935143926bb, 0x17495515121634ef, 0x48562845134b15fb, 0x36496625142725f7,
	0x27495515111836ab, 0x58642945143917bb, 0x16495525124b24ef, 0x47562835131635fb,
	0x37496615142816f3, 0x26495525113927ab, 0x58642935141734bb, 0x17495515122915ef,
	0x48562845134a26fb, 0x36496625141637f7, 0x27495515112714ab, 0x58642945143825bb,
	0x16495525121936ef, 0x47562835134717fb, 0x37496615141824f3, 0x26495525112935ab,
	0x58642935144616bb, 0x17495515123727ef, 0x48562845131834fb, 0x36496625142915f7,
	0x27495515114726ab, 0x58642945141837bb, 0x16495525122814ef, 0x47562835133925fb,
	0x37496615144736f3, 0x26495525111917ab, 0x58642935142824bb, 0x17495515124935ef,
	0x48562845131716fb, 0x36496625143827f7, 0x27495515112934ab, 0x58642945144715bb,
	0x16495525121826ef, 0x47562835132937fb, 0x37496615141714f3, 0x26495525114825ab,
	0x58642935143736bb, 0x17495515121917ef, 0x48562845132824fb, 0x36496625144935f7,
	0x27495515111716ab, 0x58642945142827bb, 0x16495525123934ef, 0x47562835134715fb,
	0x37496615141826f3, 0x26495525112937ab, 0x58642935141714bb, 0x17495515124825ef,
	0x48562845133736fb, 0x36496625141917f7, 0x27495515112824ab, 0x58642945144935bb,
	0x16495525121716ef, 0x47562835132827fb, 0x37496615143934f3, 0x26495525114715ab,
	0x58642935141826bb, 0x17495515122937ef, 0x48562845131714fb, 0x36496625144825f7,
	0x27495515113736ab, 0x58642945141917bb, 0x16495525122824ef, 0x47562835134935fb,
	0x37496615141716f3, 0x26495525112827ab, 0x58642935143934bb, 0x17495515124715ef,
	0x48562845131826fb, 0x36496625142937f7, 0x27495515111714ab, 0x58642945144825bb,
	0x16495525123736ef, 0x47562835131917fb, 0x37496615142824f3, 0x26495525114935ab,
	0x58642935141716bb, 0x17495515122827ef, 0x48562845133934fb, 0x36496625144715f7,
	0x27495515111826ab, 0x58642945142937bb, 0x16495525121714ef, 0x47562835134825fb,
	0x37496615143736f3, 0x26495525111917ab, 0x58642935142824bb, 0x17495515124935ef,
	0x48562845131716fb, 0x36496625143827f7, 0x27495515112934ab, 0x58642945144715bb,
	0x16495525121826ef, 0x47562835132937fb, 0x37496615141714f3, 0x26495525114825ab,
	0x58642935143736bb, 0x17495515121917ef, 0x48562845132824fb, 0x36496625144935f7,
	0x27495515111716ab, 0x58642945142827bb, 0x16495525123934ef, 0x47562835134715fb,
	0x37496615141826f3, 0x26495525112937ab, 0x58642935141714bb, 0x17495515124825ef,
	0x48562845133736fb, 0x36496625141917f7, 0x27495515112824ab, 0x58642945144935bb,
	0x16495525121716ef, 0x47562835132827fb, 0x37496615143934f3, 0x26495525114715ab,
	0x58642935141826bb, 0x17495515122937ef, 0x48562845131714fb, 0x36496625144825f7,
	0x27495515113736ab, 0x58642945141917bb, 0x16495525122824ef, 0x47562835134935fb,
	0x37496615141716f3, 0x26495525112827ab, 0x58642935143934bb, 0x17495515124715ef,
	0x48562845131826fb, 0x36496625142937f7, 0x27495515111714ab, 0x58642945144825bb,
	0x16495525123736ef, 0x47562835131917fb, 0x37496615142824f3, 0x26495525114935ab,
}

// Chunk represents an atomic segment of content defined by FastCDC.
type Chunk struct {
	Offset int64  `json:"offset"`
	Length int    `json:"length"`
	Hash   string `json:"hash"` // SHA-256 hex string
	Data   []byte `json:"-"`    // Byte slice payload
}

// FastCDC implements Content-Defined Chunking using the Gear matrix rolling hash.
type FastCDC struct {
	minSize       int
	avgSize       int
	maxSize       int
	normalization int
	maskS         uint64
	maskL         uint64
}

// NewFastCDC creates a new FastCDC chunker with normalized parameters.
// Normalization level 2 produces a much tighter standard deviation of chunk sizes.
func NewFastCDC(minSize, avgSize, maxSize, normalization int) *FastCDC {
	if minSize <= 0 {
		minSize = avgSize / 4
	}
	if maxSize <= avgSize {
		maxSize = avgSize * 4
	}
	if normalization <= 0 {
		normalization = 2
	}

	bits := log2(uint64(avgSize))
	// MaskS applies before reaching avgSize (stricter condition)
	maskS := (uint64(1) << (bits + uint64(normalization))) - 1
	// MaskL applies after avgSize (looser condition)
	maskL := (uint64(1) << (bits - uint64(normalization))) - 1

	return &FastCDC{
		minSize:       minSize,
		avgSize:       avgSize,
		maxSize:       maxSize,
		normalization: normalization,
		maskS:         maskS,
		maskL:         maskL,
	}
}

func log2(n uint64) uint64 {
	var r uint64
	for (n >> r) > 1 {
		r++
	}
	return r
}

// ChunkBuffer partitions a byte slice into content-defined chunks.
func (f *FastCDC) ChunkBuffer(data []byte) []Chunk {
	var chunks []Chunk
	n := len(data)
	if n == 0 {
		return chunks
	}

	var offset int64 = 0
	cursor := 0

	for cursor < n {
		remaining := n - cursor
		if remaining <= f.minSize {
			// Tail chunk
			chunkData := data[cursor:]
			chunks = append(chunks, Chunk{
				Offset: offset,
				Length: len(chunkData),
				Hash:   ComputeSHA256(chunkData),
				Data:   chunkData,
			})
			break
		}

		// Fast jump: skip minSize bytes without hashing to optimize throughput
		chunkLen := f.minSize
		var fingerprint uint64 = 0

		// Rolling hash loop over candidate region
		limit := f.maxSize
		if remaining < limit {
			limit = remaining
		}

		for chunkLen < limit {
			b := data[cursor+chunkLen]
			fingerprint = (fingerprint << 1) + GearTable[b]

			// Normalization check:
			// If chunk length is below avgSize, use maskS. Once above avgSize, switch to maskL.
			if chunkLen < f.avgSize {
				if (fingerprint & f.maskS) == 0 {
					chunkLen++
					break
				}
			} else {
				if (fingerprint & f.maskL) == 0 {
					chunkLen++
					break
				}
			}
			chunkLen++
		}

		chunkData := data[cursor : cursor+chunkLen]
		chunks = append(chunks, Chunk{
			Offset: offset,
			Length: chunkLen,
			Hash:   ComputeSHA256(chunkData),
			Data:   chunkData,
		})

		cursor += chunkLen
		offset += int64(chunkLen)
	}

	return chunks
}

// ChunkReader reads an io.Reader and streams FastCDC chunks via a callback.
func (f *FastCDC) ChunkReader(r io.Reader, onChunk func(c Chunk) error) error {
	buf := make([]byte, f.maxSize*2)
	var streamOffset int64 = 0
	buffered := 0

	for {
		// Fill buffer up to capacity
		n, err := r.Read(buf[buffered:])
		if n > 0 {
			buffered += n
		}

		if buffered == 0 && err == io.EOF {
			break
		}

		if buffered <= f.minSize && err == io.EOF {
			// Final tail chunk
			chunkData := make([]byte, buffered)
			copy(chunkData, buf[:buffered])
			c := Chunk{
				Offset: streamOffset,
				Length: buffered,
				Hash:   ComputeSHA256(chunkData),
				Data:   chunkData,
			}
			return onChunk(c)
		}

		// Find cut point in buffered data
		chunkLen := f.minSize
		if chunkLen > buffered {
			chunkLen = buffered
		}

		var fingerprint uint64 = 0
		limit := f.maxSize
		if buffered < limit {
			limit = buffered
		}

		cutFound := false
		for chunkLen < limit {
			b := buf[chunkLen]
			fingerprint = (fingerprint << 1) + GearTable[b]
			if chunkLen < f.avgSize {
				if (fingerprint & f.maskS) == 0 {
					chunkLen++
					cutFound = true
					break
				}
			} else {
				if (fingerprint & f.maskL) == 0 {
					chunkLen++
					cutFound = true
					break
				}
			}
			chunkLen++
		}

		// If hit maxSize or EOF, force cut
		if !cutFound && (chunkLen >= f.maxSize || err == io.EOF) {
			cutFound = true
		}

		if cutFound {
			chunkData := make([]byte, chunkLen)
			copy(chunkData, buf[:chunkLen])
			c := Chunk{
				Offset: streamOffset,
				Length: chunkLen,
				Hash:   ComputeSHA256(chunkData),
				Data:   chunkData,
			}

			if cbErr := onChunk(c); cbErr != nil {
				return cbErr
			}

			streamOffset += int64(chunkLen)
			// Shift remaining bytes
			copy(buf, buf[chunkLen:buffered])
			buffered -= chunkLen
		}

		if err == io.EOF && buffered == 0 {
			break
		}
		if err != nil && err != io.EOF {
			return err
		}
	}

	return nil
}

// ComputeSHA256 returns the hexadecimal SHA-256 hash of a byte slice.
func ComputeSHA256(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}
