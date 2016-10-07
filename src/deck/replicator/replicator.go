package replicator

type Replicator interface {
	Replicate(topicPath string, index int64, data *string) (ok bool)
}
