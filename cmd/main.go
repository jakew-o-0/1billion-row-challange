package main

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/cespare/xxhash"
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

type StationsMap []*Station

const MapSize = 1<<17
var buckets = 0
var stationsMap = make([]*Station, MapSize)
var stations = make([]uint64, 0)

func main() {
    f,err := os.Open("./measurements.txt")
    if err != nil {
	f,err = os.Open("../measurements.txt")
	if err != nil {
	    panic(err)
	}
    }

    i := 0
    for {
	if i >= 100_000_000 {
	    break
	}
	var chunkOffset int64 = 0
	chunk := make([]byte, 1024*1024)
	linesRead,err := f.ReadAt(chunk, chunkOffset)
	if err != nil && err != io.EOF {
	    panic(err)
	}
	if linesRead == 0 {
	    break
	}
	for i := linesRead-1; i > 0; i-- {
	    if chunk[i] == '\n' {
		chunk = chunk[:i]
		chunkOffset += int64(linesRead-i)
		break
	    }
	}

	for _,line := range bytes.Split(chunk, []byte("\n")){
	    station, tmpNum := splitLine(line)
	    num := createFixedPoint(tmpNum)
	    updateMap(station, num)
	    i++
	}
    }


    for _,i := range stations {
	v := stationsMap[i]
	mean := toFloat(v.data.sum) / toFloat(v.data.count)
	fmt.Printf("%s<%f.1/%f.1/%f.1/>\n", v.key, toFloat(v.data.min), toFloat(v.data.max), mean)
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

	if bytes.Equal(stationsMap[idx].key, station) {
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
	n = n*10 + int(b-'0')
    }

    if negative {
	return -n
    }
    return n
}

func toFloat(i int) float32 {
    return float32(i) / 10
}

func hash(key []byte) uint64 {
    var hash uint64 = 14695981039346656037
    for b := range key {
	hash ^= uint64(b)
	hash *= 1099511628211
    }
    return hash & (MapSize-1)
}

func compareBytes(a []byte, b []byte) bool {
    if len(a) != len(b) {
	return false
    }
    for i,v := range a {
	if b[i] != v {
	    return false
	}
    }
    return true
}
