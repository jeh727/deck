package deck

import (
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
	"time"
)

type Message struct {
	Header  []byte
	Payload ClientMessage
}

func NewMessage(header []byte, payload *ClientMessage) *Message {
	return &Message{Header: header, Payload: *payload}
}

func ToArray(message *Message) (array [][]byte, err error) {

	msgBytes, err := proto.Marshal(&message.Payload)
	if err != nil {
		Log.Failure.Println("Failed to marshal message")
		return nil, err
	}

	array = append(array, message.Header)
	array = append(array, []byte(""))
	array = append(array, msgBytes)

	return
}

type Topic struct {
	RequestsChan chan *Message
	StopChan     chan bool
	DoneChan     chan bool
}

func NewTopic() (topic *Topic) {

	return &Topic{
		RequestsChan: make(chan *Message, 1024),
		StopChan:     make(chan bool, 1),
		DoneChan:     make(chan bool, 1),
	}
}

type Server struct {
	StopChan chan bool
	DoneChan chan bool
	done     bool

	Topics       map[string]*Topic
	ReturnChan   chan [][]byte
	Store        *bolt.DB
	StopSyncChan chan bool
	DoneSyncChan chan bool
	Socket       *zmq.Socket
}

func NewServer(serverUrl string, boltFile string) (server *Server, err error) {

	Log.Debug.Println("NewServer: Starting server...")
	server = &Server{
		StopChan:     make(chan bool, 1),
		DoneChan:     make(chan bool, 1),
		Topics:       make(map[string]*Topic),
		ReturnChan:   make(chan [][]byte, 1024),
		StopSyncChan: make(chan bool, 1),
		DoneSyncChan: make(chan bool, 1),
	}

	// Open the db store
	server.Store, err = bolt.Open(boltFile, 0600, nil)
	if err != nil {
		Log.Failure.Printf("NewServer: Failed to open db: %s", boltFile)
		return nil, err
	}

	//Open the socket to receive requests
	server.Socket, err = zmq.NewSocket(zmq.ROUTER)
	if err != nil {
		Log.Failure.Println("NewServer: Failed to create socket")
		return
	}

	err = server.Socket.Bind(serverUrl)
	if err != nil {
		Log.Failure.Println("NewServer: Failed to bind socket")
		return
	}

	//Spawn a go routine to periodically sync the db file
	server.Store.NoSync = true
	go SyncFile(1*time.Second, server.Store, server.StopSyncChan,
		server.DoneSyncChan)

	//Start the listener
	go ServerListener(server)

	Log.Debug.Println("NewServer: Started server")
	return
}

func ServerListener(server *Server) {

	defer server.Socket.Close()

	Log.Debug.Println("ServerListener: starting processing loop...")

	stopProcessing := false

ServerLoop:
	for {
		//Process the next request if available, don't if stopping
		if !stopProcessing {
			msg, err := server.Socket.RecvMessageBytes(zmq.DONTWAIT)
			if err == nil {
				HandleRequest(server, msg)
			}
		}

		//Break out if the server is stopping
		select {
		case msg := <-server.ReturnChan:

			//Process the next response if available
			_, err := server.Socket.SendMessage(msg, 0)
			if err != nil {
				Log.Info.Println("ServerListener: Can't send response")
			}
		case <-server.StopChan:
			//Mark true to stop processing, but keep going until there are no
			//more responses available
			Log.Debug.Println("ServerListener: Telling topic workers stop...")
			TellWorkersStop(server)
			stopProcessing = true

			continue

		case <-time.After(1000 * time.Nanosecond):

			if stopProcessing && WorkersStopped(server) {
				//Stop processing is marked and there are no more responses
				Log.Debug.Println("ServerListener: Workers stopped",
					", exiting processing loop")
				break ServerLoop
			}

			//Keep processing until all of the workers have exited
			continue
		}
	}

	Log.Debug.Printf("ServerListener: Stopping file sync...")
	server.StopSyncChan <- true
	<-server.DoneSyncChan

	//do a final sync to make sure everything is flushed
	server.Store.Sync()
	Log.Debug.Printf("ServerListener: File sync stopped")

	server.DoneChan <- true
	Log.Debug.Println("ServerListener: Topic workers stopped, signaling done")
}

func SyncFile(syncInterval time.Duration, db *bolt.DB, stop chan bool,
	done chan bool) {

SyncFile:
	for {
		db.Sync()

		select {
		case <-stop:
			break SyncFile //Break out if the server is stopping
		case <-time.After(syncInterval):
			continue
		}
	}

	done <- true
}

func (server *Server) Stop() {

	if server.done {
		return
	}

	Log.Info.Println("Stopping server...")
	server.StopChan <- true
	<-server.DoneChan
	server.done = true
	Log.Info.Println("Server stopped")
}

func TellWorkersStop(server *Server) {

	for key, val := range server.Topics {
		Log.Debug.Printf("TellWorkersStop: Stopping topic: %s", key)
		val.StopChan <- true
	}
}

func WorkersStopped(server *Server) bool {

	for key, val := range server.Topics {

		select {
		case <-val.DoneChan:
			delete(server.Topics, key)
			Log.Debug.Printf("WorkersStopped: Topic Done: %s", key)
		case <-time.After(1000 * time.Nanosecond):
			//Topic not done yet, try the next one
			continue
		}
	}

	return (len(server.Topics) == 0)
}

func HandleRequest(server *Server, msg [][]byte) (err error) {

	request := &Message{}
	request.Header = msg[0]

	err = proto.Unmarshal(msg[2], &request.Payload)
	if err != nil {
		Log.Failure.Println("Failed to decode request")
		response, _ := ToArray(&Message{
			Header: request.Header,
			Payload: ClientMessage{
				Status: ClientMessage_MARSHALING_ERROR,
			},
		})

		server.ReturnChan <- response

		return
	}

	if topic, ok := server.Topics[request.Payload.Topic]; ok {
		topic.RequestsChan <- request
	} else {
		topic := NewTopic()
		server.Topics[request.Payload.Topic] = topic

		go ProcessTopic(topic, server.ReturnChan, server.Store)
		topic.RequestsChan <- request
	}

	return
}

func ProcessTopic(topic *Topic, responseChan chan [][]byte,
	topicStore *bolt.DB) {

	// todo: startup activities
	stopProcessing := false

ProcessTopic:
	for {

		var request *Message
		select {
		case request = <-topic.RequestsChan:
			//Go process the request
			break
		case <-topic.StopChan:
			stopProcessing = true
			continue
		case <-time.After(1000 * time.Nanosecond):
			if stopProcessing {
				break ProcessTopic //Break out if the server is stopping
			}
			continue
		}

		var response [][]byte
		var err error

		switch request.Payload.Operation {
		case ClientMessage_HEARTBEAT:
			response, err = HeartBeatMessage(request)
			if err != nil {
				Log.Error.Printf("HeartBeat failed: %s, %d",
					request.Payload.Topic, request.Payload.Key)
			}

		case ClientMessage_STORE:
			response, err = StoreMessage(topicStore, request)
			if err != nil {
				Log.Error.Printf("Store failed: %s, %d", request.Payload.Topic,
					request.Payload.Key)
			}

		case ClientMessage_LOAD:
			response, err = LoadMessage(topicStore, request)
			if err != nil {
				Log.Error.Printf("Load failed: %s, %d", request.Payload.Topic,
					request.Payload.Key)
			}

		case ClientMessage_CHECK_EXISTS:
			response, err = CheckMessageExists(topicStore, request)
			if err != nil {
				Log.Error.Printf("Load failed: %s, %d", request.Payload.Topic,
					request.Payload.Key)
			}

		default:
			Log.Failure.Printf("Unknown operation: %s, %d",
				request.Payload.Topic, request.Payload.Key)
			response, err = ToArray(&Message{Header: request.Header,
				Payload: ClientMessage{
					Operation: request.Payload.Operation,
					Status:    ClientMessage_MARSHALING_ERROR,
					Topic:     request.Payload.Topic,
					Key:       request.Payload.Key,
				}})
		}

		responseChan <- response
	}

	topic.DoneChan <- true
}

func HeartBeatMessage(request *Message) (response [][]byte, err error) {

	Log.Debug.Printf("Heartbeat: Processing: %s:%x", request.Payload.Topic,
		request.Payload.Key)

	resp := &Message{Header: request.Header, Payload: ClientMessage{
		Operation: ClientMessage_HEARTBEAT,
		Topic:     request.Payload.Topic,
		Key:       request.Payload.Key,
		Status:    ClientMessage_SUCCESS,
	}}

	response, _ = ToArray(resp)

	return
}

func StoreMessage(topicStore *bolt.DB, request *Message) (response [][]byte,
	err error) {

	Log.Debug.Printf("Store: Processing: %s:%x", request.Payload.Topic,
		request.Payload.Key)

	resp := &Message{Header: request.Header, Payload: ClientMessage{
		Operation: ClientMessage_STORE,
		Topic:     request.Payload.Topic,
		Key:       request.Payload.Key,
	}}

	err = topicStore.Update(func(tx *bolt.Tx) (err error) {
		bucket, err := tx.CreateBucketIfNotExists([]byte(request.Payload.Topic))
		if err != nil {
			Log.Failure.Printf("Store: Failed to open bucket: %s",
				request.Payload.Topic)
			resp.Payload.Status = ClientMessage_INTERNAL_ERROR
			response, _ = ToArray(resp)
			return
		}

		has := bucket.Get([]byte(fmt.Sprintf("%x", request.Payload.Key)))
		if has != nil {
			Log.Debug.Printf("Store: Key already in bucket: %s:%x",
				request.Payload.Topic, request.Payload.Key)
			resp.Payload.Status = ClientMessage_KEY_EXISTS
			response, _ = ToArray(resp)
			return
		}

		err = bucket.Put([]byte(fmt.Sprintf("%x", request.Payload.Key)),
			request.Payload.Value)
		if err != nil {
			Log.Failure.Printf("Store: Failed to put into bucket: %s:%x",
				request.Payload.Topic, request.Payload.Key)
			resp.Payload.Status = ClientMessage_INTERNAL_ERROR
			response, _ = ToArray(resp)
			return
		}

		resp.Payload.Status = ClientMessage_SUCCESS
		response, _ = ToArray(resp)
		return
	})

	return
}

func LoadMessage(topicStore *bolt.DB, request *Message) (response [][]byte,
	err error) {

	Log.Debug.Printf("Load: Processing: %s:%x", request.Payload.Topic,
		request.Payload.Key)

	resp := &Message{Header: request.Header, Payload: ClientMessage{
		Operation: ClientMessage_LOAD,
		Topic:     request.Payload.Topic,
		Key:       request.Payload.Key,
	}}

	err = topicStore.Update(func(tx *bolt.Tx) (err error) {
		bucket, err := tx.CreateBucketIfNotExists([]byte(request.Payload.Topic))
		if err != nil {
			Log.Failure.Printf("Load: Failed to open bucket: %s",
				request.Payload.Topic)

			resp.Payload.Status = ClientMessage_INTERNAL_ERROR
			response, _ = ToArray(resp)
			return
		}

		payload := bucket.Get([]byte(fmt.Sprintf("%x", request.Payload.Key)))
		if payload == nil {
			Log.Debug.Printf("Load: Key not in bucket: %s:%x",
				request.Payload.Topic, request.Payload.Key)

			resp.Payload.Status = ClientMessage_KEY_DOESNT_EXIST
			response, _ = ToArray(resp)
			return
		}

		resp.Payload.Value = payload
		resp.Payload.Status = ClientMessage_SUCCESS
		response, _ = ToArray(resp)

		return
	})

	return
}

func CheckMessageExists(topicStore *bolt.DB, request *Message) (
	response [][]byte, err error) {

	Log.Debug.Printf("Check: Processing: %s:%x", request.Payload.Topic,
		request.Payload.Key)

	resp := &Message{Header: request.Header, Payload: ClientMessage{
		Operation: ClientMessage_CHECK_EXISTS,
		Topic:     request.Payload.Topic,
		Key:       request.Payload.Key,
	}}

	err = topicStore.Update(func(tx *bolt.Tx) (err error) {
		bucket, err := tx.CreateBucketIfNotExists([]byte(request.Payload.Topic))
		if err != nil {
			Log.Failure.Printf("Check: Failed to open bucket: %s",
				request.Payload.Topic)

			resp.Payload.Status = ClientMessage_INTERNAL_ERROR
			response, _ = ToArray(resp)
			return
		}

		payload := bucket.Get([]byte(fmt.Sprintf("%x", request.Payload.Key)))
		if payload == nil {
			resp.Payload.Status = ClientMessage_KEY_DOESNT_EXIST
		} else {
			resp.Payload.Status = ClientMessage_KEY_EXISTS
		}

		response, _ = ToArray(resp)

		return
	})

	return
}
