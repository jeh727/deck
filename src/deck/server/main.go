package main

import (
	"deck/datastore"
	"deck/log"
	"deck/replicator"
)

func printLoad(store datastore.DataStore, topicPath string, index int64) {
	data, ok := store.Load(topicPath, index)
	if ok {
		log.Info.Println(*data)
	} else {
		log.Info.Printf("Failed to load topic: %s, index: %d", topicPath, index)
	}
}

func main() {

	log.InitDefault()

	dataStore := datastore.NewRefStore()

	printLoad(dataStore, "foo", 0)

	replicator := replicator.NewMockReplicator()

	data := "blah"
	replicator.Replicate("foo", 0, &data)
}
