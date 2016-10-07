package datastore

type DataStore interface {
	Load(topicPath string, index int64) (data *string, ok bool)
	Store(topicPath string, index int64, data *string) (ok bool)
}
