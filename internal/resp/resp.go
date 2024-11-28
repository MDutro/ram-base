package resp

import (
	"bufio"
	"fmt"
	"io"
	"ram-base/internal/types"
	"strconv"
)

const (
	STRING  = '+'
	ERROR   = '-'
	INTEGER = ':'
	BULK    = '$'
	ARRAY   = '*'
)

type Resp struct {
	reader *bufio.Reader
}

// Constructor method for a new reader
func NewResp(rd io.Reader) *Resp {
	return &Resp{reader: bufio.NewReader(rd)}
}

func (r *Resp) readLine() (line []byte, n int, err error) {
	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			return nil, 0, err
		}
		n += 1
		line = append(line, b)
		if len(line) >= 2 && line[len(line)-2] == '\r' {
			break
		}
	}
	return line[:len(line)-2], n, nil
}

func (r *Resp) readInteger() (x, n int, err error) {
	line, n, err := r.readLine()
	if err != nil {
		return 0, 0, err
	}
	i64, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, n, err
	}
	return int(i64), n, nil
}

func (r *Resp) Read() (types.Value, error) {
	_type, err := r.reader.ReadByte()

	if err != nil {
		return types.Value{}, err
	}

	switch _type {
	case ARRAY:
		return r.readArray()
	case BULK:
		return r.readBulk()
	default:
		fmt.Printf("Unkown type: %v", string(_type))
		return types.Value{}, nil
	}
}

func (r *Resp) readArray() (types.Value, error) {
	v := types.Value{}
	v.Typ = "array"

	// read length of array
	length, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	// for each line, parse and read the value
	v.Array = make([]types.Value, length)
	for i := 0; i < length; i++ {
		val, err := r.Read()
		if err != nil {
			return v, err
		}

		// add parsed value to array
		v.Array[i] = val
	}
	return v, nil
}

func (r *Resp) readBulk() (types.Value, error) {
	v := types.Value{}
	v.Typ = "bulk"

	length, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	bulk := make([]byte, length)
	r.reader.Read(bulk)
	v.Bulk = string(bulk)

	// read the trailing new line characters
	r.readLine()

	return v, nil
}

// Writer
type Writer struct {
	writer io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: w}
}

func (w *Writer) Write(v types.Value) error {
	var bytes = v.ConvertToBytes()

	_, err := w.writer.Write(bytes)
	if err != nil {
		return err
	}

	return nil
}
