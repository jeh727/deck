
package datastore

import(
    "deck/log"
    "testing"
)

func TestRefStore(t *testing.T) {

    log.InitDefault()

    store := NewRefStore()
    
    if !store.Store("foo", 0, "bar") {
        t.Error("Failed to store initial value foo:0")
    }
    
    data, ok := store.Load("foo", 0)
    if !ok {
        t.Error("Failed to load inserted value foo:0")
    }
    
    if *data != "bar" {
        t.Error("Expected value bar not return from foo:0")
    }
    
}
