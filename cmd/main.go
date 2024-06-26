package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/cespare/xxhash"
	_ "github.com/cespare/xxhash"
)


type stationData struct {
    min int
    max int
    sum int
    count int
}

type Station struct {
    key []byte
    data *stationData
}

type stationToken struct {
    station []byte
    num int
}


const MapSize = 1<<17
const ChannelSize = 64
var buckets = 0
var stationsMap = make([]*Station, MapSize)
var stations = make([]uint64, 0)


func main() {
    var masterWG sync.WaitGroup

    // chunkWorkers -> chunkChan
    chunkChan, chunkWorkersWG := CreateWorkerGroupUtils[[]byte]()
    go readChunkWorker(chunkChan, chunkWorkersWG)
    wait(&masterWG, chunkWorkersWG, chunkChan)

    // chunkChan -> tokeniseChunkWorker -> stationTokenChan
    stationTokenChan, tokeniserWG := CreateWorkerGroupUtils[stationToken]()
    wait(&masterWG, tokeniserWG, stationTokenChan)

    var chunkOffset int64 = 0
    for {
	var start int = 0
	for i,b := range chunk {
	    if b != '\n' {
		continue
	    }

	    line := chunk[start:i]
	    start = i
	    if len(line) == 0 {
		continue
	    }

	    station, tmpNum := splitLine(line)
	    num := createFixedPoint(tmpNum)
	    updateMap(station, num)
	}
    }

    for _,i := range stations {
	v := stationsMap[i]
	mean := toFloat(v.data.sum) / toFloat(v.data.count)
	fmt.Printf(
	    "%s=%.1f/%.1f/%.1f,",
	    v.key,
	    toFloat(v.data.min),
	    toFloat(v.data.max),
	    mean,
	)
    }
}

func CreateWorkerGroupUtils[T any]() (chan T, *sync.WaitGroup) {
    c := make(chan T, ChannelSize)
    w := new(sync.WaitGroup)
    return c,w
}


func wait[T any](
    masterWg *sync.WaitGroup,
    chanelWg *sync.WaitGroup,
    channel chan T,

) {
    defer masterWg.Done()
    masterWg.Add(1)

    chanelWg.Wait()
    close(channel)
}

func readChunkWorker(
    chunkChan chan<- []byte,
    chunkWorkerWG *sync.WaitGroup,
) {
    defer chunkWorkerWG.Done()


    f,err := os.Open("./measurements.txt")
    if err != nil {
	f,err = os.Open("../measurements.txt")
	if err != nil {
	    panic(err)
	}
    }
    

    var chunkOffset int64 = 0
    for {
	chunk := make([]byte, 1024*1024)
	readLines,err := f.ReadAt(chunk, chunkOffset) 
	if readLines == 0 || err != nil {
	    if err != io.EOF {
		panic(err)
	    }
	    return
	}

	var i int64 = int64(readLines-1)
	for ;i > 0; i-- {
	    if chunk[i] == '\n' {
		chunk = chunk[:i]
		chunkOffset += int64(i)
		chunkChan<- chunk
	    }
	}
    }
}

func updateMap(
    station []byte,
    num int,
) {
    idx := xxhash.Sum64(station) & (MapSize-1)
    for {
	if stationsMap[idx] == nil {
	    stationsMap[idx] = &Station{
		key: bytes.Clone(station),
		data: &stationData {
		    min: num,
		    max: num,
		    sum: num,
		    count: 1,
		},
	    }
	    
	    stations = append(stations, idx)
	    if len(stations) > 10_000 {
		panic("duplicate entry")
	    }
	    buckets++
	    if buckets >= (len(stationsMap)-1)/2 {
		panic("too many buckets")
	    }
	    break
	}

	if bytes.Equal(station, stationsMap[idx].key) {
	    s := stationsMap[idx]
	    s.data.min = min(s.data.min, num)
	    s.data.max = max(s.data.max, num)
	    s.data.sum += num
	    s.data.count++
	    stationsMap[idx] = s
	    break
	}

	idx++
	if idx >= MapSize {
	    idx = 0
	}
    }
}

func splitLine(line []byte) ([]byte, []byte) {
    length := len(line)-4
    for i := length; i > 0; i-- {
	if line[i] == ';' {
	    return line[:i], line[i+1:] 
	}
    }
    fmt.Printf("line: %s\n", line)
    panic(fmt.Errorf("invalid line. could not find ';'"))
}

func createFixedPoint(num []byte) int {
    negative := false
    n := 0
    for _,b := range num {
	if b == '-' {
	    negative = true
	    continue
	}
	if b == '.' {
	    continue
	}
	n = (n*10) + int(b-'0')
    }

    if negative {
	return -1*n
    }
    return n
}

func toFloat(i int) float32 {
    return float32(i) / 10
}
