
package main

import (
    "deck/log"
)


type Topic struct {
    data map[int64]*string
}

func NewTopic() *Topic {
    return &Topic{data: make(map[int64]*string)}
}

type DataStore struct{
    Topics map[string]*Topic
}

func NewDataStore() *DataStore{

    return &DataStore{Topics: make(map[string]*Topic)}
}

func (store *DataStore) Store(topicPath string, index int64, data string) bool {

    topic, ok := store.Topics[topicPath]
    if !ok {
        topic = NewTopic()
        store.Topics[topicPath] = topic
    }

    _, ok = topic.data[index]
    if ok {
        log.Debug.Printf("Element already exists for topic: %s, index: %d", topicPath, index)
        return false
    } else {
        topic.data[index] = &data
    }

    //TODO: persist somewhere

    return true
}

func (store *DataStore) Load(topicPath string, index int64) (data *string, ok bool) {

    topic, ok := store.Topics[topicPath]
    if !ok {
        return
    }
    
    data, ok = topic.data[index]
    if !ok {
        return
    }
    
    // TODO: load from somewhere
    return
}

func printLoad(store* DataStore, topicPath string, index int64) {
    data, ok := store.Load(topicPath, index)
    if ok {
        log.Info.Println(*data)
    } else {
        log.Info.Printf("Failed to load topic: %s, index: %d", topicPath, index)
    }
}

func main() {

    log.InitDefault()
    
    store := NewDataStore() 
    
    store.Store("foo", 0, "bar")
    printLoad(store, "foo", 0)
    printLoad(store, "foo", 1)
    
    store.Store("foo", 1, "baz")
    printLoad(store, "foo", 0)
    printLoad(store, "foo", 1)
    
}
