package main

import (
	"fmt"
	"log"
	"net/rpc"
)

type TransactionRequest struct {
	From   string
	To     string
	Amount int
}

type TransactionResponse struct {
	Success bool
	Message string
}

func main() {
	client, err := rpc.Dial("tcp", "localhost:50051")
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}

	req := TransactionRequest{
		From:   "Bob",
		To:     "Alice",
		Amount: 50,
	}

	var res TransactionResponse
	err = client.Call("Server.TransferMoney", req, &res)
	if err != nil {
		log.Fatalf("Transaction failed: %v", err)
	}

	fmt.Println("Transaction Result:", res.Message)
}
