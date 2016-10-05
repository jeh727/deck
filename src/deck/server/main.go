
package main

import (
    "deck/log"
    "deck/datastore"
)

func printLoad(store* datastore.RefStore, topicPath string, index int64) {
    data, ok := store.Load(topicPath, index)
    if ok {
        log.Info.Println(*data)
    } else {
        log.Info.Printf("Failed to load topic: %s, index: %d", topicPath, index)
    }
}

func main() {

    log.InitDefault()
}
