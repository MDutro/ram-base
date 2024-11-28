package main

import (
	"fmt"
	"net"
	"ram-base/internal/aof"
	"ram-base/internal/handler"
	"ram-base/internal/resp"
	"ram-base/internal/types"
	"strings"
)

func main() {
	fmt.Println("Listening on port :6379")

	// Create a new server
	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println(err)
		return
	}

	aof, err := aof.NewAof("database.aof")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer aof.Close()

	aof.Read(func(value types.Value) {
		command := strings.ToUpper(value.Array[0].Bulk)
		args := value.Array[1:]

		handler, ok := handler.Handlers[command]
		if !ok {
			fmt.Println("Invalid command: ", command)
			return
		}

		handler(args)
	})

	for {
		// Listen for connections
		conn, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		go handleConnection(conn, aof)
	}
}

func handleConnection(conn net.Conn, aof *aof.Aof) {
	defer conn.Close()

	for {
		respReader := resp.NewResp(conn)
		value, err := respReader.Read()
		if err != nil {
			fmt.Println(err)
			return
		}

		if value.Typ != "array" {
			fmt.Println("Invalid request, expected array")
			continue
		}

		if len(value.Array) == 0 {
			fmt.Println("Invalid request, expected array length > 0")
			continue
		}

		command := strings.ToUpper(value.Array[0].Bulk)
		// fmt.Println(command)
		args := value.Array[1:]

		writer := resp.NewWriter(conn)

		handler, ok := handler.Handlers[command]
		if !ok {
			fmt.Println("Invalid command: ", command)
			writer.Write(types.Value{Typ: "string", Str: ""})
			continue
		}

		if command == "SET" || command == "HSET" {
			aof.Write(value)
		}

		result := handler(args)
		writer.Write(result)
	}
}
