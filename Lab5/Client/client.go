package main

import (
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strconv"

	"Lab5/models"
)

func main() {
	if len(os.Args) < 5 {
		log.Fatalf("Usage: go run client.go <coordinator_addr> <from_account> <to_account> <amount>")
	}

	coordinatorAddr := os.Args[1]
	fromAccount := os.Args[2]
	toAccount := os.Args[3]
	amount, err := strconv.Atoi(os.Args[4])
	if err != nil {
		log.Fatalf("Invalid amount: %v", err)
	}

	// Kết nối tới Coordinator
	client, err := rpc.Dial("tcp", coordinatorAddr)
	if err != nil {
		log.Fatalf("Failed to connect to Coordinator: %v", err)
	}
	defer client.Close()

	// Tạo yêu cầu giao dịch
	req := models.TransactionRequest{From: fromAccount, To: toAccount, Amount: amount}
	var res models.TransactionResponse

	// Gửi yêu cầu giao dịch đến Coordinator
	err = client.Call("Coordinator.TransferMoney", req, &res)
	if err != nil {
		log.Fatalf("Transaction failed: %v", err)
	}

	fmt.Printf("Transaction Result: %v\n", res.Message)
}
