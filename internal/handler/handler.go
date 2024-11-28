package handler

import (
	"ram-base/internal/types"
	"sync"
)

var Handlers = map[string]func([]types.Value) types.Value{
	"PING":    ping,
	"SET":     set,
	"GET":     get,
	"HSET":    hset,
	"HGET":    hget,
	"HGETALL": hgetall,
}

func ping(args []types.Value) types.Value {
	if len(args) == 0 {
		return types.Value{Typ: "string", Str: "PONG"}
	}

	return types.Value{Typ: "string", Str: args[0].Bulk}
}

var SETs = map[string]string{}
var SETsMu = sync.RWMutex{}

func set(args []types.Value) types.Value {
	if len(args) != 2 {
		return types.Value{Typ: "error", Str: "ERR wrong number of arguments for 'set' command"}
	}

	key := args[0].Bulk
	value := args[1].Bulk

	SETsMu.Lock()
	SETs[key] = value
	SETsMu.Unlock()

	return types.Value{Typ: "string", Str: "OK"}
}

func get(args []types.Value) types.Value {
	if len(args) != 1 {
		return types.Value{Typ: "error", Str: "ERR wrong number of arguments for 'get' command"}
	}

	key := args[0].Bulk

	SETsMu.RLock()
	value, ok := SETs[key]
	SETsMu.RUnlock()

	if !ok {
		return types.Value{Typ: "null"}
	}

	return types.Value{Typ: "bulk", Bulk: value}
}

var HSETs = map[string]map[string]string{}
var HSETsMu = sync.RWMutex{}

func hset(args []types.Value) types.Value {
	if len(args) != 3 {
		return types.Value{Typ: "error", Str: "ERR wrong number of arguments for 'hset' command"}
	}

	hash := args[0].Bulk
	key := args[1].Bulk
	value := args[2].Bulk

	HSETsMu.Lock()
	if _, ok := HSETs[hash]; !ok {
		HSETs[hash] = map[string]string{}
	}
	HSETs[hash][key] = value
	HSETsMu.Unlock()

	return types.Value{Typ: "string", Str: "OK"}
}

func hget(args []types.Value) types.Value {
	if len(args) != 2 {
		return types.Value{Typ: "error", Str: "ERR wrong number of arguments for 'hget' command"}
	}

	hash := args[0].Bulk
	key := args[1].Bulk

	HSETsMu.RLock()
	value, ok := HSETs[hash][key]
	HSETsMu.RUnlock()

	if !ok {
		return types.Value{Typ: "null"}
	}

	return types.Value{Typ: "bulk", Bulk: value}
}

func hgetall(args []types.Value) types.Value {
	if len(args) != 1 {
		return types.Value{Typ: "error", Str: "ERR wrong number of arguments for 'hgetall' command"}
	}

	hash := args[0].Bulk

	HSETsMu.RLock()
	value, ok := HSETs[hash]
	HSETsMu.RUnlock()

	if !ok {
		return types.Value{Typ: "null"}
	}

	values := []types.Value{}
	for k, v := range value {
		values = append(values, types.Value{Typ: "bulk", Bulk: k})
		values = append(values, types.Value{Typ: "bulk", Bulk: v})
	}

	return types.Value{Typ: "array", Array: values}
}
