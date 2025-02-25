package go_srf

import (
	"encoding/binary"
	"errors"
	"io"
)

var ErrInvalidStartOffset = errors.New("invalid start offset")
var ErrInvalidCount = errors.New("invalid count")

// Count return the total number of records in the reader
// - all records are read/validated
func Count(src io.Reader) (int64, error) {
	var total int64 = 0
	for {
		if _, err := Read(src); err != nil {
			if errors.Is(err, io.EOF) {
				break
			} else {
				return 0, err
			}
		}
		total++
	}
	return total, nil
}

// Extract reads count records from src from start offset; start offset starts at 0
// if allowPrematureEnd is true, function can return partial results if EOF
func Extract(src io.Reader, start int64, count int64, allowPrematureEnd bool) ([]Record, error) {
	if start < 0 {
		return nil, ErrInvalidStartOffset
	}
	if count < 1 {
		return nil, ErrInvalidCount
	}

	result := make([]Record, 0)
	// first, attempt to skip
	if start > 0 {
		for {
			if err := skipRead(src); err != nil {
				if errors.Is(err, io.EOF) {
					if allowPrematureEnd {
						return result, nil
					} else {
						// return eof error
						return nil, err
					}
				} else {
					// some other error occurred
					return nil, err
				}
			}
			start--
			if start == 0 {
				break
			}
		}
	}

	for {
		if r, err := Read(src); err != nil {
			if errors.Is(err, io.EOF) {
				if allowPrematureEnd {
					return result, nil
				} else {
					// return eof error
					return nil, err
				}
			} else {
				// some other error occurred
				return nil, err
			}
		} else {
			result = append(result, r)
		}
		count--
		if count == 0 {
			return result, nil
		}
	}
}

// Copy copies count records from src from start offset and writes them into dest; start offset starts at 0
// if allowPrematureEnd is true, function can return partial results if EOF
func Copy(src io.Reader, dst io.Writer, start int64, count int64, compress bool, allowPrematureEnd bool) error {
	if start < 0 {
		return ErrInvalidStartOffset
	}
	if count < 1 {
		return ErrInvalidCount
	}

	// first, attempt to skip
	if start > 1 {
		for {
			if err := skipRead(src); err != nil {
				if errors.Is(err, io.EOF) {
					if allowPrematureEnd {
						return nil
					} else {
						// return eof error
						return err
					}
				} else {
					// some other error occurred
					return err
				}
			}
			start--
			if start == 0 {
				break
			}
		}
	}

	for {
		if r, err := Read(src); err != nil {
			if errors.Is(err, io.EOF) {
				if allowPrematureEnd {
					return nil
				} else {
					// return eof error
					return err
				}
			} else {
				// some other error occurred
				return err
			}
		} else {
			if err = WriteRecord(dst, r, compress); err != nil {
				return err
			}
		}
		count--
		if count == 0 {
			return nil
		}
	}
}

// skipRead reads and skips a record to advance the read offset
func skipRead(src io.Reader) error {
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

	var szMeta uint32
	var szData uint64
	if err := binary.Read(src, binary.LittleEndian, &szMeta); err != nil {
		return err
	}
	if err := binary.Read(src, binary.LittleEndian, &szData); err != nil {
		return err
	}
	if szMeta > 0 {
		buf := make([]byte, szMeta)
		if _, err := src.Read(buf); err != nil {
			return err
		}
	}
	if szData > 0 {
		buf := make([]byte, szData)
		if _, err := src.Read(buf); err != nil {
			return err
		}
	}
	return nil
}
