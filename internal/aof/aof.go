package aof

import (
	"bufio"
	"io"
	"os"
	"ram-base/internal/resp"
	"ram-base/internal/types"
	"sync"
	"time"
)

type Aof struct {
	file *os.File
	rd   *bufio.Reader
	mu   sync.Mutex
}

func NewAof(path string) (*Aof, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	aof := &Aof{
		file: f,
		rd:   bufio.NewReader(f),
	}

	// Start a goroutine to sync AOF to disk every 1 second
	go func() {
		for {
			// fmt.Println("Syncing to file...")
			aof.mu.Lock()
			aof.file.Sync()
			aof.mu.Unlock()
			time.Sleep(time.Second)
		}
	}()

	return aof, nil
}

func (aof *Aof) Close() error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	return aof.file.Close()
}

func (aof *Aof) Write(value types.Value) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	_, err := aof.file.Write(value.ConvertToBytes())
	if err != nil {
		return err
	}

	return nil
}

func (aof *Aof) Read(callback func(value types.Value)) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	aof.file.Seek(0, io.SeekStart)

	resp := resp.NewResp(aof.file)

	for {
		value, err := resp.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		callback(value)
	}
	return nil
}
