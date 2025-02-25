package go_srf

import (
	"bytes"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
)

func testCount(t *testing.T, compress bool) {
	buf := new(bytes.Buffer)
	dataset := testDataset()

	for _, r := range dataset {
		err := Write(buf, r.RecordType, r.Data, r.Meta, false)
		assert.NoError(t, err)
	}

	assert.True(t, buf.Len() > 1024)

	// test count
	reader := bytes.NewReader(buf.Bytes())
	total, err := Count(reader)
	assert.NoError(t, err)
	assert.Equal(t, len(dataset), int(total))
}

func TestCount(t *testing.T) {
	testCount(t, false)
	testCount(t, true)
}

func testExtractEmpty(t *testing.T, compress bool) {
	var err error

	// empty buffer
	reader := bytes.NewReader(new(bytes.Buffer).Bytes())

	// invalid start, should return error
	_, err = Extract(reader, -1, 10, false)
	assert.ErrorIs(t, ErrInvalidStartOffset, err)

	// invalid count
	_, err = Extract(reader, 1, 0, false)
	assert.ErrorIs(t, ErrInvalidCount, err)

	// EOF, should return error
	_, err = Extract(reader, 1, 10, false)
	assert.ErrorIs(t, io.EOF, err)

	// empty buffer with allowPrematureEnd==true should return empty slice
	var rows []Record
	rows, err = Extract(reader, 1, 10, true)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(rows))
}

func bufDataset(t *testing.T, ds []sampleRecord) *bytes.Buffer {
	buf := new(bytes.Buffer)

	for _, r := range ds {
		err := Write(buf, r.RecordType, r.Data, r.Meta, false)
		assert.NoError(t, err)
	}
	return buf
}

func testExtractSimple(t *testing.T, compress bool) {
	ds := testDataset()
	buf := bufDataset(t, ds)

	// extract first record
	rows, err := Extract(bytes.NewReader(buf.Bytes()), 0, 1, false)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(rows))
	assert.Equal(t, ds[0].Data, rows[0].Bytes())

	// extract 3 records starting at record 3
	rows, err = Extract(bytes.NewReader(buf.Bytes()), 3, 3, false)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(rows))
	for i := 0; i < 3; i++ {
		assert.Equal(t, ds[3+i].Data, rows[i].Bytes())
		if ds[3+i].Meta != nil {
			meta, err := json.Marshal(ds[3+i].Meta)
			assert.NoError(t, err)
			assert.Equal(t, meta, rows[i].Meta())
		}
	}

	// extract 3 records starting at record 4
	rows, err = Extract(bytes.NewReader(buf.Bytes()), 4, 3, false)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(rows))
	for i := 0; i < 3; i++ {
		assert.Equal(t, ds[4+i].Data, rows[i].Bytes())
		if ds[3+i].Meta != nil {
			meta, err := json.Marshal(ds[4+i].Meta)
			assert.NoError(t, err)
			assert.Equal(t, meta, rows[i].Meta())
		}
	}
}

func testExtractEnd(t *testing.T, compress bool) {
	ds := testDataset()
	buf := bufDataset(t, ds)

	// attempt to extract 3 records, but only 2 exist
	// allowPrematureEnd is false, so should raise error
	rows, err := Extract(bytes.NewReader(buf.Bytes()), 5, 3, false)
	assert.Error(t, io.EOF)

	// attempt to extract 3 records, but only 2 exist
	// allowPrematureEnd is true, should work
	rows, err = Extract(bytes.NewReader(buf.Bytes()), 6, 3, true)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(rows))
	for i := 0; i < 2; i++ {
		assert.Equal(t, ds[6+i].Data, rows[i].Bytes())
		if ds[6+i].Meta != nil {
			meta, err := json.Marshal(ds[6+i].Meta)
			assert.NoError(t, err)
			assert.Equal(t, meta, rows[i].Meta())
		}
	}
}

func TestExtract(t *testing.T) {
	// empty buffer
	testExtractEmpty(t, false)
	testExtractEmpty(t, true)

	// extraction
	testExtractSimple(t, false)
	testExtractSimple(t, true)

	testExtractEnd(t, false)
	testExtractEnd(t, true)
}

func testCopySimple(t *testing.T, compress bool) {
	ds := testDataset()
	buf := bufDataset(t, ds)
	src := bytes.NewReader(buf.Bytes())
	dest := new(bytes.Buffer)

	// copy 4th record to new buffer
	_, err := src.Seek(0, io.SeekStart)
	assert.NoError(t, err)
	err = Copy(src, dest, 3, 1, compress, false)
	assert.NoError(t, err)

	// verify dest
	var total int64
	destReader := bytes.NewReader(dest.Bytes())
	total, err = Count(destReader)
	assert.NoError(t, err)
	assert.Equal(t, total, int64(1))

	// read copied record
	var r Record
	_, err = destReader.Seek(0, io.SeekStart)
	assert.NoError(t, err)
	r, err = Read(destReader)
	assert.NoError(t, err)
	assert.Equal(t, r.Type(), ds[3].RecordType)
	assert.Equal(t, r.Bytes(), ds[3].Data)
}

func testCopyMultiple(t *testing.T, compress bool) {
	ds := testDataset()
	buf := bufDataset(t, ds)
	src := bytes.NewReader(buf.Bytes())
	dest := new(bytes.Buffer)

	// copy 3 records starting at 6th record to new buffer
	_, err := src.Seek(0, io.SeekStart)
	assert.NoError(t, err)
	err = Copy(src, dest, 5, 3, compress, false)
	assert.NoError(t, err)

	// verify dest
	var total int64
	destReader := bytes.NewReader(dest.Bytes())
	total, err = Count(destReader)
	assert.NoError(t, err)
	assert.Equal(t, total, int64(3))

	// read copied record
	var r Record
	_, err = destReader.Seek(0, io.SeekStart)
	assert.NoError(t, err)
	for i := 0; i < 3; i++ {
		r, err = Read(destReader)
		assert.NoError(t, err)
		assert.Equal(t, r.Type(), ds[5+i].RecordType)
		assert.Equal(t, r.Bytes(), ds[5+i].Data)
	}
}

func testCopyPrematureEnd(t *testing.T, compress bool) {
	ds := testDataset()
	buf := bufDataset(t, ds)
	src := bytes.NewReader(buf.Bytes())
	dest := new(bytes.Buffer)

	// copy 3 records starting at 7th record to new buffer
	// should only copy 2 records
	_, err := src.Seek(0, io.SeekStart)
	assert.NoError(t, err)
	err = Copy(src, dest, 6, 3, compress, true)
	assert.NoError(t, err)

	// verify dest
	var total int64
	destReader := bytes.NewReader(dest.Bytes())
	total, err = Count(destReader)
	assert.NoError(t, err)
	assert.Equal(t, total, int64(2))

	// read copied record
	var r Record
	_, err = destReader.Seek(0, io.SeekStart)
	assert.NoError(t, err)
	for i := 0; i < 2; i++ {
		r, err = Read(destReader)
		assert.NoError(t, err)
		assert.Equal(t, r.Type(), ds[6+i].RecordType)
		assert.Equal(t, r.Bytes(), ds[6+i].Data)
	}
}

func TestCopy(t *testing.T) {
	testCopySimple(t, false)
	testCopySimple(t, true)

	testCopyMultiple(t, false)
	testCopyMultiple(t, true)

	testCopyPrematureEnd(t, false)
	testCopyPrematureEnd(t, true)

}
