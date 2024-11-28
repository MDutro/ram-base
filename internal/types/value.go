package types

import (
	"strconv"
)

type Value struct {
	Typ   string  // determine data type carried by the value
	Str   string  // holds string received from simple strings
	Num   int     // holds value of the integer received from integers
	Bulk  string  // holds string received from bulk strings
	Array []Value // holds all values received from arrays
}

const (
	STRING  = '+'
	ERROR   = '-'
	INTEGER = ':'
	BULK    = '$'
	ARRAY   = '*'
)

// Convert value to bytes depending on type
func (v Value) ConvertToBytes() []byte {
	switch v.Typ {
	case "array":
		return v.convertArray()
	case "bulk":
		return v.convertBulk()
	case "string":
		return v.convertString()
	case "null":
		return v.convertNull()
	case "error":
		return v.convertError()
	default:
		return []byte{}
	}
}

func (v Value) convertString() []byte {
	var bytes []byte
	bytes = append(bytes, STRING)
	bytes = append(bytes, v.Str...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

func (v Value) convertBulk() []byte {
	var bytes []byte
	bytes = append(bytes, BULK)
	bytes = append(bytes, strconv.Itoa(len(v.Bulk))...)
	bytes = append(bytes, '\r', '\n')
	bytes = append(bytes, v.Bulk...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

func (v Value) convertArray() []byte {
	len := len(v.Array)
	var bytes []byte
	bytes = append(bytes, ARRAY)
	bytes = append(bytes, strconv.Itoa(len)...)
	bytes = append(bytes, '\r', '\n')

	for i := 0; i < len; i++ {
		bytes = append(bytes, v.Array[i].ConvertToBytes()...)
	}

	return bytes
}

func (v Value) convertError() []byte {
	var bytes []byte
	bytes = append(bytes, ERROR)
	bytes = append(bytes, v.Str...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

func (v Value) convertNull() []byte {
	return []byte("$-1\r'\n")
}
