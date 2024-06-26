package attempt1

import (
	"os"
	"strconv"
	"strings"
	"sync"
)


const minDefault float64 = (1<<64)-1


func generateChunks(
    resultChan chan<- []byte,
    offset int64,
    wg *sync.WaitGroup,
) {
    defer wg.Done()

    // open file
    f,err := os.Open("./measurements.txt")
    if err != nil { 
        f,err = os.Open("../measurements.txt")
        if err != nil {
            panic(err)
        }
    }
    defer f.Close()

    // create buffer
    for range 1_000_000 {
        buff := make([]byte, chunckSize)
        bytesRead,_ := f.ReadAt(buff, offset)

        i := bytesRead-1
        for range 2 {
            if buff[i] == '\n' {
                break
            }
            i--
        }

        offset = int64(bytesRead) - int64(i)
        resultChan <- buff
    }
}


func readChunk(
    recieveChunks <-chan []byte,
    output chan<- StationToken,
    wg *sync.WaitGroup,
) {
    defer wg.Done()
    for buff := range recieveChunks {
        TokenizeBuff(buff, output)
    }
}

func TokenizeBuff(buff []byte, output chan<- StationToken) {
    start := 0
    for i,b := range buff {
        if b != '\n' {
            continue
        }
        // slice from begining to i\
        line := buff[start:i]
        start = i+1

        sarr := strings.Split(string(line), ";")
        num,err := strconv.ParseFloat(sarr[1], 32)
        if err != nil {
            panic(err)
        }

        tok :=  StationToken {
            Station: sarr[0],
            Num: num,
        }

        output<- tok 
    }
}

/*
func tokenizeString(
    recieveStrings <-chan string,
    output chan<- StationToken,
    wg *sync.WaitGroup,
) {
    defer wg.Done()
    for s := range recieveStrings {
        // parse slice
    }
}

*/

func collectTokens (
    recieveTokens <-chan StationToken,
    output chan<- StationDataMap,
    wg *sync.WaitGroup,
) {
    defer wg.Done()
    tokenMap := make(map[string]StationData)

    for token := range recieveTokens {
        t,ok := tokenMap[token.Station]
        if !ok {
            t = StationData{
                min: token.Num,
                max: token.Num,
                count: 1,
                total: token.Num,
            }
            tokenMap[token.Station] = t
            continue
        }

        t.count++
        t.total += token.Num
        if t.min > token.Num {
            t.min = token.Num
        }
        if t.max < token.Num {
            t.max = token.Num
        }
        tokenMap[token.Station] = t
    }
    output <- tokenMap   
}

func FinalCollect(
    aggrigateResults []*chan StationDataMap,
    result chan<- StationDataMap, 
    wg *sync.WaitGroup,
) {
    defer wg.Done()
    final := make(StationDataMap)
    for _,a := range aggrigateResults {
        for r := range *a {
            final = Collect(r, final)
        }
    }
    result <- final
}

func Collect(
    cur StationDataMap,
    final StationDataMap,
) StationDataMap {
    for k,v := range cur {
        t,ok := final[k]
        if !ok {
            final[k] = v
            continue
        }

        t.count += v.count
        t.total += v.total
        if t.min > v.min {
            t.min = v.min
        }
        if t.max < v.max {
            t.max = v.max
        }
        final[k] = t
    }
    return final
}

