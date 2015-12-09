package udf

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/gogo/protobuf/proto"
)

type ByteReadReader interface {
	io.Reader
	io.ByteReader
}

type ByteReadCloser interface {
	io.ReadCloser
	io.ByteReader
}

// Write the message to the io.Writer with a varint size header.
func WriteMessage(msg proto.Message, w io.Writer) error {
	// marshal message
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	varint := make([]byte, binary.MaxVarintLen32)
	n := binary.PutUvarint(varint, uint64(len(data)))

	w.Write(varint[:n])
	w.Write(data)
	return nil
}

// Read a message from io.ByteReader by first reading a varint size,
// and then reading and decoding the message object.
// If buf is not big enough a new buffer will be allocated to replace buf.
func ReadMessage(buf *[]byte, r ByteReadReader, msg proto.Message) error {
	size, err := binary.ReadUvarint(r)
	if err != nil {
		return err
	}
	if cap(*buf) < int(size) {
		*buf = make([]byte, size)
	}
	b := (*buf)[:size]
	n, err := r.Read(b)
	if err == io.EOF {
		return fmt.Errorf("unexpected EOF, expected %d more bytes", size)
	}
	if err != nil {
		return err
	}
	if n != int(size) {
		return fmt.Errorf("unexpected EOF, expected %d more bytes", int(size)-n)
	}
	err = proto.Unmarshal(b, msg)
	if err != nil {
		return err
	}
	return nil
}
