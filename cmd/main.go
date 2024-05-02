package main

import (
	"bufio"
	"fmt"
	"os"
)


type stationData struct {
    min int
    max int
    sum int
    count int
}


func main() {
    f,err := os.Open("./measurements.txt")
    if err != nil {
	f,err = os.Open("../measurements.txt")
	if err != nil {
	    panic(err)
	}
    }

    stationsMap := make(map[string]*stationData)
    r := bufio.NewScanner(f)
    for r.Scan() {
	tmpStation, tmpNum := splitLine(r.Bytes())
	station := string(tmpStation)
	num := createFixedPoint(tmpNum)

	s := stationsMap[station]
	s = updateEntry(s, num)
	stationsMap[station] = s
    }
}


func splitLine(line []byte) ([]byte, []byte) {
    length := len(line)-4
    for i := length; i > 0; i-- {
	if line[i] != ';' {
	    continue
	}
	return line[:i], line[i:] 
    }
    panic(fmt.Errorf("invalid line. could not find ';'"))
}

func createFixedPoint(num []byte) int {
    negative := false
    n := 0

    for _,b := range num {
	if b == '-' {
	    negative = true
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

func updateEntry(entry *stationData, num int) *stationData {
    if entry != nil {
	entry.min = min(entry.min, num)
	entry.max = min(entry.max, num)
	entry.sum += num
	entry.count++
	return entry
    }

    return &stationData {
	min: num,
	max: num,
	sum: num,
	count: 1,
    }
}
