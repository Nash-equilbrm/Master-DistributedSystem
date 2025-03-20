package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	mongoURI1  = "mongodb://localhost:27017/?replicaSet=rs0"
	mongoURI2  = "mongodb://localhost:27018/?replicaSet=rs0"
	database   = "bank"
	collection = "accounts"
)

// Request và Response cho giao dịch
type TransactionRequest struct {
	From   string
	To     string
	Amount int
}

type TransactionResponse struct {
	Success bool
	Message string
}

// Server chứa kết nối tới 2 MongoDB nodes
type Server struct {
	client1 *mongo.Client // Node 1
	client2 *mongo.Client // Node 2
}

// Thực hiện giao dịch sử dụng 2PC
func (s *Server) TransferMoney(req TransactionRequest, res *TransactionResponse) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Tạo session cho từng MongoDB node
	session1, err := s.client1.StartSession()
	if err != nil {
		res.Success = false
		res.Message = "Failed to start session on primary"
		return err
	}
	defer session1.EndSession(ctx)

	session2, err := s.client2.StartSession()
	if err != nil {
		res.Success = false
		res.Message = "Failed to start session on secondary"
		return err
	}
	defer session2.EndSession(ctx)

	// Callback thực hiện giao dịch trên cả hai nodes
	callback := func(sessCtx mongo.SessionContext) (interface{}, error) {
		coll1 := s.client1.Database(database).Collection(collection)
		coll2 := s.client2.Database(database).Collection(collection)

		// Giai đoạn Chuẩn bị (Prepare Phase)
		log.Println("Prepare Phase: Checking balances")
		var fromAccount struct {
			Balance int `bson:"balance"`
		}
		err := coll1.FindOne(sessCtx, bson.M{"_id": req.From}).Decode(&fromAccount)
		if err != nil {
			return nil, fmt.Errorf("failed to find source account: %w", err)
		}
		if fromAccount.Balance < req.Amount {
			return nil, fmt.Errorf("insufficient balance")
		}

		// Giai đoạn Cam kết (Commit Phase)
		log.Println("Commit Phase: Executing transactions")
		_, err = coll1.UpdateOne(sessCtx, bson.M{"_id": req.From}, bson.M{"$inc": bson.M{"balance": -req.Amount}})
		if err != nil {
			return nil, fmt.Errorf("failed to deduct amount: %w", err)
		}
		_, err = coll2.UpdateOne(sessCtx, bson.M{"_id": req.To}, bson.M{"$inc": bson.M{"balance": req.Amount}})
		if err != nil {
			return nil, fmt.Errorf("failed to credit amount: %w", err)
		}

		return nil, nil
	}

	// Chạy giao dịch trên cả hai MongoDB nodes
	log.Println("Starting transaction across two nodes")
	_, err = session1.WithTransaction(ctx, callback)
	_, err = session2.WithTransaction(ctx, callback)

	if err != nil {
		res.Success = false
		res.Message = "Transaction failed"
		log.Println("Transaction failed:", err)
		return err
	}

	res.Success = true
	res.Message = "Transaction completed successfully"
	log.Println("Transaction committed successfully")
	return nil
}

// Khởi chạy server RPC
func startServer() {
	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	// Kết nối tới hai MongoDB nodes
	server := new(Server)
	server.client1, err = mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURI1))
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB Node 1: %v", err)
	}
	server.client2, err = mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURI2))
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB Node 2: %v", err)
	}

	// Tạo server RPC
	rpcServer := rpc.NewServer()
	rpcServer.Register(server)

	log.Println("Server is running on port 50051")
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Connection error:", err)
			continue
		}
		go rpcServer.ServeConn(conn)
	}
}

func main() {
	startServer()
}
