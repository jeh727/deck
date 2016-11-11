package deck

import (
	"bytes"
	"fmt"
	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
	"os"
	"testing"
)

func ClientSend(requester *zmq.Socket, request *ClientMessage) (response *ClientMessage, err error) {

	msgBytes, err := proto.Marshal(request)
	if err != nil {
		Log.Error.Println("Failed to marshal request")
		return
	}
	requestMsg := [][]byte{}
	requestMsg = append(requestMsg, msgBytes)

	//Log.Debug.Println("Sending Store Request")
	_, err = requester.SendMessage(requestMsg, 0)
	if err != nil {
		Log.Error.Println("Failed to send request")
		return
	}

	responseMsg, err := requester.RecvMessageBytes(0)
	if err != nil {
		Log.Error.Println("Failed to receive response")
		return
	}

	response = &ClientMessage{}
	err = proto.Unmarshal(responseMsg[0], response)
	if err != nil {
		Log.Debug.Println("Failed to decode Request")
	}

	return
}

//"tcp://localhost:5555"
func ClientConnect(serverUrl string) (socket *zmq.Socket, err error) {

	//  Socket to talk to server
	Log.Info.Println("Connecting to server...")
	socket, err = zmq.NewSocket(zmq.REQ)
	if err != nil {
		return
	}

	err = socket.Connect(serverUrl)
	if err != nil {
		socket.Close()
		socket = nil
		return
	}

	return
}

//"tcp://*:5555"
func ServerStart(dbName string, serverUrl string) (server *Server, err error) {

	err = os.Remove(dbName)
	if err != nil {
		Log.Info.Println("Failed to remove old db, test may fail")
	}

	server, err = NewServer(serverUrl, dbName)
	if err != nil {
		return
	}
	return
}

func TestServerStop(t *testing.T) {

	InitDebugLogging()

	t.Log("Starting server")
	server, err := ServerStart("test.db", "tcp://*:5555")
	if err != nil {
		t.Error("Failed to start server")
		return
	}
	defer os.Remove("test.db")

	//Note: There is another stop below,this will test that stop can be called more then once safely
	defer server.Stop()

	requester, err := ClientConnect("tcp://localhost:5555")
	if err != nil {
		t.Error("Failed to open connection to server")
		return
	}
	defer requester.Close()

	t.Log("Sending store message...")
	// Load value that hasn't been stored yet
	request := &ClientMessage{
		Operation: ClientMessage_STORE,
		Topic:     "/test/topic",
		Key:       int64(0),
		Value:     make([]byte, 1000),
	}

	_, err = ClientSend(requester, request)
	if err != nil {
		t.Error("Failed to send message")
	}

	t.Log("Stopping server...")
	server.Stop()
}

func TestServerBasicOps(t *testing.T) {

	InitDefaultLogging()

	t.Log("Starting server")
	server, err := ServerStart("test.db", "tcp://*:5555")
	if err != nil {
		t.Log("Failed to start server")
	}
	defer os.Remove("test.db")
	defer server.Stop()

	requester, err := ClientConnect("tcp://localhost:5555")
	if err != nil {
		t.Error("Failed to open connection to server")
	}
	defer requester.Close()

	//TEST 1: Try and load a non existent message
	// Load value that hasn't been stored yet
	request := &ClientMessage{
		Operation: ClientMessage_LOAD,
		Topic:     "/test/topic",
		Key:       int64(42),
	}

	response, err := ClientSend(requester, request)
	if err != nil {
		t.Error("Failed to send message")
	}

	if response.Operation != ClientMessage_LOAD {
		t.Error("Invalid operation returned")
	}
	if response.Topic != "/test/topic" {
		t.Error("Invalid topic returned: ", response.Topic)
	}
	if response.Key != int64(42) {
		t.Error("Invalid key returned")
	}
	if response.Status != ClientMessage_KEY_DOESNT_EXIST {
		t.Error("Didn't get expected status(KEY_DOESNT_EXIST")
	}

	//TEST 2: Check a non existent message
	// doesn't exist
	request = &ClientMessage{
		Operation: ClientMessage_CHECK_EXISTS,
		Topic:     "/test/topic",
		Key:       int64(42),
	}

	response, err = ClientSend(requester, request)
	if err != nil {
		t.Error("Failed to send message")
	}

	if response.Operation != ClientMessage_CHECK_EXISTS {
		t.Error("Invalid operation returned")
	}
	if response.Topic != "/test/topic" {
		t.Error("Invalid topic returned: ", response.Topic)
	}
	if response.Key != int64(42) {
		t.Error("Invalid key returned")
	}
	if response.Status != ClientMessage_KEY_DOESNT_EXIST {
		t.Error("Didn't get expected status KEY_DOESNT_EXIST")
	}

	//TEST 3: Try and store a new key
	request = &ClientMessage{
		Operation: ClientMessage_STORE,
		Topic:     "/test/topic",
		Key:       int64(42),
		Value:     []byte("TEST 3"),
	}

	response, err = ClientSend(requester, request)
	if err != nil {
		t.Error("Failed to send message")
	}

	if response.Operation != ClientMessage_STORE {
		t.Error("Invalid operation returned")
	}
	if response.Topic != "/test/topic" {
		t.Error("Invalid topic returned: ", response.Topic)
	}
	if response.Key != int64(42) {
		t.Error("Invalid key returned")
	}
	if response.Status != ClientMessage_SUCCESS {
		t.Error("Didn't get expected status SUCCESS")
	}

	//TEST 4: Try and store an existing key
	request = &ClientMessage{
		Operation: ClientMessage_STORE,
		Topic:     "/test/topic",
		Key:       int64(42),
		Value:     []byte("TEST 4"),
	}

	response, err = ClientSend(requester, request)
	if err != nil {
		t.Error("Failed to send message")
	}

	if response.Operation != ClientMessage_STORE {
		t.Error("Invalid operation returned")
	}
	if response.Topic != "/test/topic" {
		t.Error("Invalid topic returned: ", response.Topic)
	}
	if response.Key != int64(42) {
		t.Error("Invalid key returned")
	}
	if response.Status != ClientMessage_KEY_EXISTS {
		t.Error("Didn't get expected status KEY_EXISTS")
	}

	//TEST 5: Try and load an existing key
	request = &ClientMessage{
		Operation: ClientMessage_LOAD,
		Topic:     "/test/topic",
		Key:       int64(42),
	}

	response, err = ClientSend(requester, request)
	if err != nil {
		t.Error("Failed to send message")
	}

	if response.Operation != ClientMessage_LOAD {
		t.Error("Invalid operation returned")
	}
	if response.Topic != "/test/topic" {
		t.Error("Invalid topic returned: ", response.Topic)
	}
	if response.Key != int64(42) {
		t.Error("Invalid key returned")
	}
	if response.Status != ClientMessage_SUCCESS {
		t.Error("Didn't get expected status SUCCESS")
	}
	if !bytes.Equal(response.Value, []byte("TEST 3")) {
		t.Error("Invalid key returned")
	}

	//TEST 6: Check an existing message
	// doesn't exist
	request = &ClientMessage{
		Operation: ClientMessage_CHECK_EXISTS,
		Topic:     "/test/topic",
		Key:       int64(42),
	}

	response, err = ClientSend(requester, request)
	if err != nil {
		t.Error("Failed to send message")
	}

	if response.Operation != ClientMessage_CHECK_EXISTS {
		t.Error("Invalid operation returned")
	}
	if response.Topic != "/test/topic" {
		t.Error("Invalid topic returned: ", response.Topic)
	}
	if response.Key != int64(42) {
		t.Error("Invalid key returned")
	}
	if response.Status != ClientMessage_KEY_EXISTS {
		t.Error("Didn't get expected status KEY_EXISTS")
	}
}

func BenchmarkStore(b *testing.B) {

	InitSilentLogging()

	b.StopTimer()
	server, err := ServerStart("test.db", "tcp://*:5555")
	if err != nil {
		b.Error("Failed to start server")
		return
	}
	defer os.Remove("test.db")
	defer server.Stop()

	requester, err := ClientConnect("tcp://localhost:5555")
	if err != nil {
		b.Error("Failed to open connection to server")
	}
	defer requester.Close()

	for request_nbr := 0; request_nbr != b.N; request_nbr++ {

		for topicNum := 0; topicNum < 1; topicNum++ {
			request := &ClientMessage{
				Operation: ClientMessage_STORE,
				Topic:     fmt.Sprintf("/test/%x", topicNum),
				Key:       int64(request_nbr),
				Value:     make([]byte, 1000),
			}

			b.StartTimer()
			ClientSend(requester, request)
			b.StopTimer()
		}

		//Log.Debug.Println("Received Store Response")
	}
}

func BenchmarkLoad(b *testing.B) {

	InitSilentLogging()

	b.StopTimer()
	server, err := ServerStart("test.db", "tcp://*:5555")
	if err != nil {
		b.Error("Failed to start server")
		return
	}
	defer os.Remove("test.db")
	defer server.Stop()

	requester, err := ClientConnect("tcp://localhost:5555")
	if err != nil {
		b.Error("Failed to open connection to server")
	}
	defer requester.Close()

	for request_nbr := 0; request_nbr != b.N; request_nbr++ {

		for topicNum := 0; topicNum < 1; topicNum++ {
			request := &ClientMessage{
				Operation: ClientMessage_STORE,
				Topic:     fmt.Sprintf("/test/%x", topicNum),
				Key:       int64(request_nbr),
				Value:     make([]byte, 1000),
			}

			ClientSend(requester, request)
		}
	}

	for request_nbr := 0; request_nbr != b.N; request_nbr++ {

		for topicNum := 0; topicNum < 1; topicNum++ {
			request := &ClientMessage{
				Operation: ClientMessage_LOAD,
				Topic:     fmt.Sprintf("/test/%x", topicNum),
				Key:       int64(request_nbr),
			}

			b.StartTimer()
			ClientSend(requester, request)
			b.StopTimer()
		}
	}
}

func BenchmarkCheckExists(b *testing.B) {

	InitSilentLogging()

	b.StopTimer()
	server, err := ServerStart("test.db", "tcp://*:5555")
	if err != nil {
		b.Error("Failed to start server")
		return
	}
	defer os.Remove("test.db")
	defer server.Stop()

	requester, err := ClientConnect("tcp://localhost:5555")
	if err != nil {
		b.Error("Failed to open connection to server")
	}
	defer requester.Close()

	for request_nbr := 0; request_nbr != b.N; request_nbr++ {

		for topicNum := 0; topicNum < 1; topicNum++ {
			request := &ClientMessage{
				Operation: ClientMessage_STORE,
				Topic:     fmt.Sprintf("/test/%x", topicNum),
				Key:       int64(request_nbr),
				Value:     make([]byte, 1000),
			}

			ClientSend(requester, request)
		}
	}

	for request_nbr := 0; request_nbr != b.N; request_nbr++ {

		for topicNum := 0; topicNum < 1; topicNum++ {
			request := &ClientMessage{
				Operation: ClientMessage_CHECK_EXISTS,
				Topic:     fmt.Sprintf("/test/%x", topicNum),
				Key:       int64(request_nbr),
			}

			b.StartTimer()
			ClientSend(requester, request)
			b.StopTimer()
		}
	}
}

func BenchmarkHeartbeat(b *testing.B) {

	InitSilentLogging()

	b.StopTimer()
	server, err := ServerStart("test.db", "tcp://*:5555")
	if err != nil {
		b.Error("Failed to start server")
		return
	}
	defer os.Remove("test.db")
	defer server.Stop()

	requester, err := ClientConnect("tcp://localhost:5555")
	if err != nil {
		b.Error("Failed to open connection to server")
	}
	defer requester.Close()

	for request_nbr := 0; request_nbr != b.N; request_nbr++ {

		for topicNum := 0; topicNum < 1; topicNum++ {
			request := &ClientMessage{
				Operation: ClientMessage_HEARTBEAT,
				Topic:     fmt.Sprintf("/test/%x", topicNum),
				Key:       int64(request_nbr),
			}

			b.StartTimer()
			ClientSend(requester, request)
			b.StopTimer()
		}
	}
}
