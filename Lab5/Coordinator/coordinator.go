package main

import (
	"Lab5/models"
	"log"
	"net"
	"net/rpc"
	"os"
)

type Coordinator struct {
	server1 *rpc.Client
	server2 *rpc.Client
}

func (c *Coordinator) TransferMoney(req models.TransactionRequest, res *models.TransactionResponse) error {
	log.Printf("[Coordinator] Starting 2PC transaction: %s → %s, Amount: %d", req.From, req.To, req.Amount)

	fromServer := c.server1
	toServer := c.server2

	if req.From == "account_2" {
		fromServer = c.server2
		toServer = c.server1
	}

	// Phase 1: Prepare
	log.Printf("[Coordinator] Requesting Prepare from %s", req.From)
	err := fromServer.Call("Server.PrepareTransaction", req, res)
	if err != nil || !res.Success {
		log.Printf("[Coordinator] ERROR: Prepare failed for %s, rolling back...", req.From)
		fromServer.Call("Server.RollbackTransaction", req, res)
		return err
	}

	log.Printf("[Coordinator] Requesting Prepare from %s", req.To)
	err = toServer.Call("Server.PrepareTransaction", req, res)
	if err != nil || !res.Success {
		log.Printf("[Coordinator] ERROR: Prepare failed for %s, rolling back...", req.To)
		fromServer.Call("Server.RollbackTransaction", req, res)
		toServer.Call("Server.RollbackTransaction", req, res)
		return err
	}

	// Phase 2: Commit
	log.Printf("[Coordinator] Requesting Commit from %s", req.From)
	err = fromServer.Call("Server.CommitTransaction", req, res)
	if err != nil || !res.Success {
		log.Printf("[Coordinator] ERROR: Commit failed for %s, rolling back...", req.From)
		toServer.Call("Server.RollbackTransaction", req, res)
		return err
	}

	log.Printf("[Coordinator] Requesting Commit from %s", req.To)
	err = toServer.Call("Server.CommitTransaction", req, res)
	if err != nil || !res.Success {
		log.Printf("[Coordinator] ERROR: Commit failed for %s, rolling back...", req.To)
		fromServer.Call("Server.RollbackTransaction", req, res)
		return err
	}

	log.Printf("[Coordinator] SUCCESS: Transaction completed!")
	return nil
}

func startCoordinator(server1Addr, server2Addr, port string) {
	server1, err := rpc.Dial("tcp", server1Addr)
	if err != nil {
		log.Fatalf("Failed to connect to server1: %v", err)
	}

	server2, err := rpc.Dial("tcp", server2Addr)
	if err != nil {
		log.Fatalf("Failed to connect to server2: %v", err)
	}

	coordinator := &Coordinator{
		server1: server1,
		server2: server2,
	}

	rpcServer := rpc.NewServer()
	rpcServer.Register(coordinator)

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Failed to start coordinator: %v", err)
	}

	log.Printf("[Coordinator] Running on port %s", port)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("[Coordinator] Connection error:", err)
			continue
		}
		go rpcServer.ServeConn(conn)
	}
}

func main() {
	if len(os.Args) < 4 {
		log.Fatalf("Usage: go run coordinator.go <server1_addr> <server2_addr> <port>")
	}

	server1Addr := os.Args[1]
	server2Addr := os.Args[2]
	port := os.Args[3]

	startCoordinator(server1Addr, server2Addr, port)
}
