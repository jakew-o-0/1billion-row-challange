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

type StationChanData struct {
    StationMap []*Station
    stationsIdxList []uint64
}

const MapSize = 1<<17
var buckets = 0


func main() {
    f,err := os.Open("./measurements.txt")
    if err != nil {
	f,err = os.Open("../measurements.txt")
	if err != nil {
	    panic(err)
	}
    }

    stationsChan := make(chan StationChanData, 64)
    var stationsWg sync.WaitGroup

    // parse measurements.txt
    var chunkOffset int64 = 0
    for {
	chunk,breakSig := readChunk(f, &chunkOffset)
	if breakSig {
	    break
	}
	go parseLineWorker(&stationsWg, stationsChan, chunk)
    }

    // fan-in the workers
    stationsMap := make([]*Station, MapSize)
    stations := make([]uint64, 0)
    for res := range stationsChan{
	for _,idx := range res.stationsIdxList {
	    combineMap(
		stationsMap,
		stations,
		res.StationMap[idx].key,
		res.StationMap[idx].data,
	    )
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


func parseLineWorker(wg *sync.WaitGroup, stationChan chan<- StationChanData, chunk []byte) {
    defer wg.Done()
    wg.Add(1)

    stationsMap := make([]*Station, MapSize)
    stations := make([]uint64, 0)
    start := 0

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
	updateMap(stationsMap, stations, station, num)
    }

    stationChan<- StationChanData {
	StationMap: stationsMap,
	stationsIdxList: stations,

    }
}

func readChunk(f *os.File, chunkOffset *int64) ([]byte, bool) {
    chunk := make([]byte, 1024*1024)
    readLines,err := f.ReadAt(chunk, *chunkOffset) 
    if readLines == 0 || err != nil {
	if err != io.EOF {
	    panic(err)
	}
	return nil, true
    }

    var i int64 = int64(readLines-1)
    for ;i > 0; i-- {
	if chunk[i] == '\n' {
	    chunk = chunk[:i]
	    *chunkOffset += int64(i)
	    return chunk, false
	}
    }
    return nil,true
}

func updateMap(
    stationsMap []*Station,
    stations []uint64,
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

func combineMap(
    stationsMap []*Station,
    stations []uint64,
    station []byte,
    data *stationData,
) {
    idx := xxhash.Sum64(station) & (MapSize-1)
    for {
	if stationsMap[idx] == nil {
	    stationsMap[idx] = &Station{
		key: bytes.Clone(station),
		data: data,
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
	    s.data.min = min(s.data.min, data.min)
	    s.data.max = max(s.data.max, data.max)
	    s.data.sum += data.sum
	    s.data.count += data.count
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
