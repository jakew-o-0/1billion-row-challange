package main

import (
	"fmt"
	"sync"
)

type StationToken struct {
    Station string
    Num float64
}

type StationData struct {
    min float64
    max float64
    count int
    total float64
}

type StationDataMap map[string]StationData

const fileSize = 13472020
const channelSize = 64
const chunckSize int64 = 64 * 1024
const TotalStations = 10_000
const ColectLikeMapSize = 100

func main() {

    // master sync
    masterWg := new(sync.WaitGroup)

    // generate chunks
    chunkChan,chunkWg := CreateWorkerGroup[[]byte]()
    chunkWg.Add(1)
    go generateChunks(chunkChan, 0, chunkWg)
    go wait(masterWg, chunkWg, chunkChan)


    // read each incoming chunk
    // each chunk has its own pool of workers for tokenising and collecting
    readChunkChan,readChunkWg := CreateWorkerGroup[string]()
    chunkResultChans := new([]*chan StationDataMap)
    CreateReadChunkWorkers( readChunkWg, masterWg, readChunkChan, chunkChan, chunkResultChans)
    go wait(masterWg, readChunkWg, readChunkChan)

    
    // collect all aggrigated results into a single map
    collectChan, collectWg := CreateWorkerGroup[StationDataMap]()
    collectWg.Add(1)
    go FinalCollect(*chunkResultChans, collectChan, collectWg)
    go wait(masterWg, collectWg, collectChan)

    masterWg.Wait()
    fmt.Printf("%+v", <-collectChan)
}

// creating workers
func CreateReadChunkWorkers(
    readChunkWg *sync.WaitGroup,
    masterWg *sync.WaitGroup,
    readChunkChan chan string,
    chunkChan <-chan []byte,
    chunkResultChans *[]*chan StationDataMap,
) {
    readChunkWg.Add(channelSize)
    for range channelSize {
        go readChunk(chunkChan, readChunkChan, readChunkWg)

        // tokenizer workers
        tokensChan,tokenizerWg := CreateWorkerGroup[StationToken]()
        tokenizerWg.Add(channelSize)
        go CreateTokenizerWorkers(tokenizerWg, readChunkChan, tokensChan)
        go wait(masterWg, tokenizerWg, tokensChan)

        //collector workers
        chunkResultChan, collectorWg := CreateWorkerGroup[StationDataMap]()
        *chunkResultChans = append(*chunkResultChans, &chunkResultChan)
        collectorWg.Add(1)
        go collectTokens(tokensChan, chunkResultChan, collectorWg)
        go wait(masterWg, collectorWg, chunkResultChan)
    }
}

func CreateTokenizerWorkers(
    tokenizerWg *sync.WaitGroup,
    readChunkChan <-chan string,
    tokensChan chan<- StationToken,
) {
    for range channelSize {
        go tokenizeString(readChunkChan, tokensChan, tokenizerWg)
    }
}

func CreateCollectorWorkers(
    collectorWg *sync.WaitGroup,
    collectorChan chan<- StationDataMap,
    tokensChan <-chan StationToken,
) {
    collectorWg.Add(channelSize)
    for range channelSize {
    }
}





// generic helper funcs
func CreateWorkerGroup[T any]() (chan T, *sync.WaitGroup) {
    c := make(chan T, channelSize)
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
