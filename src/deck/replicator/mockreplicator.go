package replicator

type MockReplicator struct {
}

func (mock *MockReplicator) Replicate(topicPath string, index int64, data *string) (ok bool) {

	return true
}

func NewMockReplicator() *MockReplicator {

	return new(MockReplicator)
}
