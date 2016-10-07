package datastore

type topicData struct {
	data map[int64]*string
}

func newTopicData() *topicData {
	return &topicData{data: make(map[int64]*string)}
}

type RefStore struct {
	topics map[string]*topicData
}

func NewRefStore() *RefStore {
	return &RefStore{topics: make(map[string]*topicData)}
}

func (store *RefStore) Store(topicPath string, index int64, data *string) (ok bool) {

	topic, ok := store.topics[topicPath]
	if !ok {
		topic = newTopicData()
		store.topics[topicPath] = topic
	}

	_, ok = topic.data[index]
	if ok {
		return false
	} else {
		topic.data[index] = data
	}

	//TODO: persist somewhere

	return true
}

func (store *RefStore) Load(topicPath string, index int64) (data *string, ok bool) {

	topic, ok := store.topics[topicPath]
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
