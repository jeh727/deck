
package datastore

import(
    "deck/log"
    "fmt"
    "testing"
)

func TestRefStore(t *testing.T) {

    log.InitDefault()

    store := NewRefStore()
    var ok bool
    var data *string
    
    topics := []string{"foo", "bar", "rah", "blah"}
    
    // Test over topics.  New topics should start empty
    for _,topic := range(topics) {
        
        for storeIndex := int64(0); storeIndex < 10; storeIndex++ {
            
            for loadIndex := int64(0); loadIndex < 10; loadIndex++ {
                data, ok = store.Load(topic, loadIndex)
                if loadIndex < storeIndex {    
                    if !ok {
                        t.Error("Failed to load inserted value: ", loadIndex)
                    }else if *data != fmt.Sprintf("%d", loadIndex) {
                        t.Error("Loaded unexpected data: ", *data)
                    }
                } else if ok {
                    t.Error("Unepectedly loaded value: ", loadIndex)
                } 
            }
            
            for storeIndex2 := int64(0); storeIndex2 < storeIndex; storeIndex2++ {
                //Store
                if store.Store(topic, storeIndex2, fmt.Sprintf("%d", storeIndex2)) {
                    t.Error("Stored already inserted value: %d", storeIndex2)
                }
            }
            
            //Store
            if !store.Store(topic, storeIndex, fmt.Sprintf("%d", storeIndex)) {
                t.Error("Failed to store value: %d", storeIndex)
            }
        }
    }
}

func BenchmarkRefStoreStore(b *testing.B) {
    log.InitDefault()

    store := NewRefStore()
    
    // Store N items
    for index := int64(0); index < int64(b.N); index++ {
        store.Store("foo", index, fmt.Sprintf("%d", index))
    }
}

func BenchmarkRefStoreLoad(b *testing.B) {
    log.InitDefault()

    store := NewRefStore()
    
    // Store N items
    for index := int64(0); index < int64(100); index++ {
        store.Store("foo", index, fmt.Sprintf("%d", index))
    }
    
    // Load N items
    for index := int64(0); index < int64(b.N); index++ {
        store.Load("foo", index % 100)
    }
}
    