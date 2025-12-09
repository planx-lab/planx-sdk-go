// Package batch provides zero-copy batch encoding/decoding for Planx plugins.
// All transport MUST use Batch - no single-record transport allowed.
package batch

import (
	"encoding/binary"
	"errors"
)

// Record represents a single data record in the pipeline.
// Payload MUST remain opaque binary - Engine MUST NOT parse it.
type Record struct {
	Metadata map[string]string
	Payload  []byte // opaque binary (zero-copy)
}

// Batch represents a collection of records for batch transport.
// All transport MUST use Batch.
type Batch struct {
	Records []Record
	Context map[string]string
}

// Packed batch format:
// [4 bytes: magic number 0x504C4E58 "PLNX"]
// [4 bytes: version]
// [4 bytes: record count]
// [4 bytes: context count]
// [context entries: key_len(2) + key + value_len(2) + value]
// [for each record:
//    [4 bytes: metadata count]
//    [metadata entries: key_len(2) + key + value_len(2) + value]
//    [4 bytes: payload length]
//    [payload bytes]
// ]

const (
	magicNumber uint32 = 0x504C4E58 // "PLNX"
	version     uint32 = 1
)

// PackBatch encodes a Batch into a packed binary format.
// This format is designed for zero-copy transport.
func PackBatch(b Batch) ([]byte, error) {
	// Estimate size (rough calculation for initial allocation)
	estimatedSize := 16 // header
	for _, r := range b.Records {
		estimatedSize += 8 + len(r.Payload)
		for k, v := range r.Metadata {
			estimatedSize += 4 + len(k) + len(v)
		}
	}
	for k, v := range b.Context {
		estimatedSize += 4 + len(k) + len(v)
	}

	buf := make([]byte, 0, estimatedSize)

	// Header
	buf = binary.BigEndian.AppendUint32(buf, magicNumber)
	buf = binary.BigEndian.AppendUint32(buf, version)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(b.Records)))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(b.Context)))

	// Context
	for k, v := range b.Context {
		buf = appendString(buf, k)
		buf = appendString(buf, v)
	}

	// Records
	for _, r := range b.Records {
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(r.Metadata)))
		for k, v := range r.Metadata {
			buf = appendString(buf, k)
			buf = appendString(buf, v)
		}
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(r.Payload)))
		buf = append(buf, r.Payload...)
	}

	return buf, nil
}

// UnpackBatch decodes a packed binary format into a Batch.
// This uses zero-copy where possible (payload slices reference input buffer).
func UnpackBatch(data []byte) (Batch, error) {
	if len(data) < 16 {
		return Batch{}, errors.New("batch: data too short")
	}

	offset := 0

	// Validate magic number
	magic := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	if magic != magicNumber {
		return Batch{}, errors.New("batch: invalid magic number")
	}

	// Check version
	ver := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	if ver != version {
		return Batch{}, errors.New("batch: unsupported version")
	}

	recordCount := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	contextCount := binary.BigEndian.Uint32(data[offset:])
	offset += 4

	// Parse context
	context := make(map[string]string, contextCount)
	for i := uint32(0); i < contextCount; i++ {
		key, newOffset, err := readString(data, offset)
		if err != nil {
			return Batch{}, err
		}
		offset = newOffset

		value, newOffset, err := readString(data, offset)
		if err != nil {
			return Batch{}, err
		}
		offset = newOffset

		context[key] = value
	}

	// Parse records
	records := make([]Record, recordCount)
	for i := uint32(0); i < recordCount; i++ {
		if offset+4 > len(data) {
			return Batch{}, errors.New("batch: unexpected end of data")
		}

		metadataCount := binary.BigEndian.Uint32(data[offset:])
		offset += 4

		metadata := make(map[string]string, metadataCount)
		for j := uint32(0); j < metadataCount; j++ {
			key, newOffset, err := readString(data, offset)
			if err != nil {
				return Batch{}, err
			}
			offset = newOffset

			value, newOffset, err := readString(data, offset)
			if err != nil {
				return Batch{}, err
			}
			offset = newOffset

			metadata[key] = value
		}

		if offset+4 > len(data) {
			return Batch{}, errors.New("batch: unexpected end of data")
		}
		payloadLen := binary.BigEndian.Uint32(data[offset:])
		offset += 4

		if offset+int(payloadLen) > len(data) {
			return Batch{}, errors.New("batch: payload extends beyond data")
		}

		// Zero-copy: slice references the original buffer
		payload := data[offset : offset+int(payloadLen)]
		offset += int(payloadLen)

		records[i] = Record{
			Metadata: metadata,
			Payload:  payload,
		}
	}

	return Batch{
		Records: records,
		Context: context,
	}, nil
}

func appendString(buf []byte, s string) []byte {
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(s)))
	return append(buf, s...)
}

func readString(data []byte, offset int) (string, int, error) {
	if offset+2 > len(data) {
		return "", 0, errors.New("batch: string length extends beyond data")
	}
	length := binary.BigEndian.Uint16(data[offset:])
	offset += 2

	if offset+int(length) > len(data) {
		return "", 0, errors.New("batch: string extends beyond data")
	}
	s := string(data[offset : offset+int(length)])
	return s, offset + int(length), nil
}
