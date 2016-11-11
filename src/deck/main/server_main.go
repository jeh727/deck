package main

import (
	"deck"
)

func main() {
	deck.InitDefaultLogging()

	server, err := deck.NewServer("tcp://*:5555", "deck.db")
	if err != nil {
		deck.Log.Error.Println("Failed to start server")
	}
	defer server.Stop()

}
