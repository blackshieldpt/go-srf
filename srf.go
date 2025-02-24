package go_srf

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"github.com/klauspost/compress/zstd"
	"io"
	"reflect"
)

const (
	Magic = "SRF0"

	// ZStd concurrency
	DecoderConcurrency = 1
	EncoderConcurrency = 1

	// SRF base types
	TypeBinary = 1
	TypeText   = 2
	TypeJSON   = 3
)

// Record SRF record
type Record interface {
	Type() uint16
	HasMeta() bool
	Meta() []byte
	Bytes() []byte
	String() string
}

type record struct {
	recordType uint16
	meta       []byte
	body       []byte
}

var (
	// Read Errors
	ErrInvalidHeader   = errors.New("invalid srf header")
	ErrInvalidZeroBits = errors.New("garbage found in reserved bits")
)

func validateHeader(r io.Reader) error {
	buf := make([]byte, 4)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return err
	}
	if string(buf) != Magic {
		return ErrInvalidHeader
	}
	return nil
}

func (r *record) read(src io.Reader) error {
	if err := validateHeader(src); err != nil {
		return err
	}

	// process compression & type
	var v uint32
	if err := binary.Read(src, binary.LittleEndian, &v); err != nil {
		return err
	}
	// check if reserved bits are zero
	if ((v >> 16) & 0x7F) != 0 {
		return ErrInvalidZeroBits
	}

	// extract type & compression
	r.recordType = uint16(v & 0xFF)
	compressed := (v>>31)&0x1 == 1

	var szMeta uint32
	var szData uint64
	if err := binary.Read(src, binary.LittleEndian, &szMeta); err != nil {
		return err
	}
	if err := binary.Read(src, binary.LittleEndian, &szData); err != nil {
		return err
	}

	var d *zstd.Decoder
	var err error
	if szMeta > 0 || compressed {
		d, err = zstd.NewReader(src, zstd.WithDecoderConcurrency(DecoderConcurrency))
		if err != nil {
			return err
		}
		defer d.Close()
	}

	// read meta
	if szMeta > 0 {
		buf := make([]byte, szMeta)
		if _, err = io.ReadFull(src, buf); err != nil {
			return err
		}
		if r.meta, err = d.DecodeAll(buf, nil); err != nil {
			return err
		}
	}

	// read body
	if compressed {
		buf := make([]byte, szData)
		if _, err := io.ReadFull(src, buf); err != nil {
			return err
		}
		if r.body, err = d.DecodeAll(buf, nil); err != nil {
			return err
		}
	} else {
		r.body = make([]byte, szData)
		if _, err := io.ReadFull(src, r.body); err != nil {
			return err
		}
	}

	return nil
}

// Type returns Record type
func (r *record) Type() uint16 {
	return r.recordType
}

// HasMeta returns true if metadata exists
func (r *record) HasMeta() bool {
	return len(r.meta) > 0
}

// Meta returns metadata the byte stream, if exists; it is a byte-encoded JSON string
// To unpack a record metadata to a struct, use the helper function UnpackMeta()
func (r *record) Meta() []byte {
	return r.meta
}

// Bytes return the record payload
func (r *record) Bytes() []byte {
	return r.body
}

// String returns the record payload as a string
func (r *record) String() string {
	return string(r.body)
}

// Read reads a record from a io.Reader
// if read is successful, returns a new record; if not, returns the error
func Read(src io.Reader) (Record, error) {
	r := &record{}
	if err := r.read(src); err != nil {
		return nil, err
	}
	return r, nil
}

// ReadAll reads a list of records from an io.Reader and returns the slice of records
func ReadAll(src io.Reader) ([]Record, error) {
	var records []Record
	for {
		rec, err := Read(src)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
	}
	return records, nil
}

// Write writes a new record, field by field, to an io.Writer
// - if the record has no metadata, meta value should be nil;
// - if compress is true, data is compressed using Zstd before it is written;
func Write(dst io.Writer, recordType uint16, data []byte, meta any, compress bool) error {
	var buf []byte
	var rawMeta []byte
	var rawRecord []byte
	var enc *zstd.Encoder
	var err error
	hasMeta := meta != nil && !reflect.ValueOf(meta).IsNil()

	// initialize compressor if necessary
	if hasMeta || compress {
		enc, err = zstd.NewWriter(dst, zstd.WithEncoderConcurrency(EncoderConcurrency), zstd.WithEncoderCRC(true))
		if err != nil {
			return err
		}
	}

	if hasMeta {
		if buf, err = json.Marshal(meta); err != nil {
			return err
		}
		// compress meta
		rawMeta = enc.EncodeAll(buf, nil)
	}

	if compress {
		rawRecord = enc.EncodeAll(data, nil)
	} else {
		rawRecord = data
	}

	return RawWrite(dst, recordType, rawMeta, rawRecord, compress)
}

// WriteString helper Write() function to write a text record
func WriteString(dst io.Writer, recordType uint16, data string, meta any, compress bool) error {
	return Write(dst, recordType, []byte(data), meta, compress)
}

// RawWrite write low-level record to stream
//   - this function only performs the low-level write operation; metadata should already be compressed, and rawRecord
//  may already be the compressed data, if isCompressed is true;
//   - passing wrong parameters to the function will result in invalid records being written;
func RawWrite(dst io.Writer, rType uint16, rawMeta []byte, rawRecord []byte, isCompressed bool) error {
	// write header
	if err := binary.Write(dst, binary.LittleEndian, []byte(Magic)); err != nil {
		return err
	}

	// type & flags
	t := uint32(rType) & 0xFF
	if isCompressed {
		t = t | (0x01 << 31)
	}

	var lenMeta uint32
	if rawMeta != nil && len(rawMeta) > 0 {
		lenMeta = uint32(len(rawMeta))
	} else {
		lenMeta = 0
	}
	lenRecord := uint64(len(rawRecord))

	// write type
	if err := binary.Write(dst, binary.LittleEndian, t); err != nil {
		return err
	}

	// write meta size
	if err := binary.Write(dst, binary.LittleEndian, lenMeta); err != nil {
		return err
	}

	// write body size
	if err := binary.Write(dst, binary.LittleEndian, lenRecord); err != nil {
		return err
	}

	// write meta
	if lenMeta > 0 {
		if _, err := dst.Write(rawMeta); err != nil {
			return err
		}
	}

	// write body
	if _, err := dst.Write(rawRecord); err != nil {
		return err
	}
	return nil
}

// WriteRecord writes a Record struct to a io.Writer
func WriteRecord(dst io.Writer, r Record, compress bool) error {
	var rawMeta []byte
	var rawRecord []byte
	var enc *zstd.Encoder
	var err error

	meta := r.Meta()
	hasMeta := meta != nil && len(meta) > 0

	// initialize compressor if necessary
	if hasMeta || compress {
		enc, err = zstd.NewWriter(dst, zstd.WithEncoderConcurrency(EncoderConcurrency), zstd.WithEncoderCRC(true))
		if err != nil {
			return err
		}
	}

	if hasMeta {
		// compress meta
		rawMeta = enc.EncodeAll(meta, nil)
	}

	rawRecord = r.Bytes()
	if compress {
		rawRecord = enc.EncodeAll(rawRecord, nil)
	}

	return RawWrite(dst, r.Type(), rawMeta, rawRecord, compress)
}

// UnpackMeta extract JSON metadata into v if metadata exists
func UnpackMeta(r Record, v any) error {
	if r.HasMeta() {
		return json.Unmarshal(r.Meta(), v)
	}
	return nil
}

// DecodeJson extract JSON body from a Record
func DecodeJson(r Record, v any) error {
	return json.Unmarshal(r.Bytes(), v)
}
