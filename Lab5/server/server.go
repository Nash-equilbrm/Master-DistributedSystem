package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"Lab5/models"
)

const (
	database   = "bank"
	collection = "accounts"
)

type Server struct {
	client    *mongo.Client
	accountID string
	prepared  map[string]int // Lưu trạng thái các giao dịch đang chờ commit
}

// Phase 1: Prepare - Kiểm tra số dư và "giữ" tiền trước khi commit
func (s *Server) PrepareTransaction(req models.TransactionRequest, res *models.TransactionResponse) error {
	log.Printf("[Server %s] Preparing transaction: %s → %s, Amount: %d", s.accountID, req.From, req.To, req.Amount)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	coll := s.client.Database(database).Collection(collection)

	// Kiểm tra số dư tài khoản nguồn
	if req.From == s.accountID {
		var sender struct {
			Balance int `bson:"balance"`
		}
		err := coll.FindOne(ctx, bson.M{"_id": req.From}).Decode(&sender)
		if err != nil {
			res.Success = false
			res.Message = "Source account not found"
			log.Printf("[Server %s] ERROR: Source account not found!", s.accountID)
			return err
		}
		log.Printf("[Server %s] Current balance of %s: %d", s.accountID, req.From, sender.Balance)

		if sender.Balance < req.Amount {
			res.Success = false
			res.Message = "Insufficient balance"
			log.Printf("[Server %s] ERROR: Insufficient balance in %s", s.accountID, req.From)
			return fmt.Errorf("insufficient balance")
		}
	}

	// Đánh dấu giao dịch đã được chuẩn bị nhưng chưa commit
	s.prepared[req.From] = req.Amount
	log.Printf("[Server %s] Transaction prepared: %s → %s, Amount: %d", s.accountID, req.From, req.To, req.Amount)

	res.Success = true
	res.Message = "Transaction prepared successfully"
	return nil
}

// Phase 2: Commit - Thực hiện giao dịch nếu tất cả các bên đều chuẩn bị xong
func (s *Server) CommitTransaction(req models.TransactionRequest, res *models.TransactionResponse) error {
	log.Printf("[Server %s] Committing transaction: %s → %s, Amount: %d", s.accountID, req.From, req.To, req.Amount)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	coll := s.client.Database(database).Collection(collection)

	// Thực hiện giao dịch
	_, err := coll.UpdateOne(ctx, bson.M{"_id": req.From}, bson.M{"$inc": bson.M{"balance": -req.Amount}})
	if err != nil {
		res.Success = false
		res.Message = "Failed to debit source account"
		log.Printf("[Server %s] ERROR: Failed to debit %s", s.accountID, req.From)
		return err
	}

	_, err = coll.UpdateOne(ctx, bson.M{"_id": req.To}, bson.M{"$inc": bson.M{"balance": req.Amount}})
	if err != nil {
		// Nếu cộng tiền vào tài khoản đích thất bại, rollback ngay
		log.Printf("[Server %s] ERROR: Failed to credit %s, rolling back...", s.accountID, req.To)
		s.RollbackTransaction(req, res)
		return err
	}

	// Xóa trạng thái chuẩn bị
	delete(s.prepared, req.From)

	res.Success = true
	res.Message = "Transaction committed successfully"
	log.Printf("[Server %s] SUCCESS: Transaction committed!", s.accountID)
	return nil
}

// Phase 2 (Fail Case): Rollback nếu Commit thất bại
func (s *Server) RollbackTransaction(req models.TransactionRequest, res *models.TransactionResponse) error {
	log.Printf("[Server %s] Rolling back transaction: %s → %s, Amount: %d", s.accountID, req.From, req.To, req.Amount)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	coll := s.client.Database(database).Collection(collection)

	// Kiểm tra xem giao dịch có trong danh sách `prepared` không
	if amount, exists := s.prepared[req.From]; exists {
		// Hoàn trả số tiền đã trừ trước đó
		_, err := coll.UpdateOne(ctx, bson.M{"_id": req.From}, bson.M{"$inc": bson.M{"balance": amount}})
		if err != nil {
			res.Success = false
			res.Message = "Rollback failed"
			log.Printf("[Server %s] ERROR: Rollback failed for %s", s.accountID, req.From)
			return err
		}

		// Xóa khỏi danh sách `prepared`
		delete(s.prepared, req.From)
		log.Printf("[Server %s] Rollback successful!", s.accountID)

		res.Success = true
		res.Message = "Transaction rolled back successfully"
	} else {
		log.Printf("[Server %s] WARNING: No prepared transaction found to rollback!", s.accountID)
		res.Success = false
		res.Message = "No prepared transaction found"
	}
	return nil
}

func startServer(mongoURI, port, accountID string) {
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Failed to listen on port %s: %v", port, err)
	}

	server := &Server{accountID: accountID, prepared: make(map[string]int)}
	server.client, err = mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer server.client.Disconnect(context.Background())

	rpcServer := rpc.NewServer()
	rpcServer.Register(server)

	log.Printf("[Server %s] Running on port %s, connected to MongoDB at %s", accountID, port, mongoURI)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("[Server] Connection error:", err)
			continue
		}
		go rpcServer.ServeConn(conn)
	}
}

func main() {
	if len(os.Args) < 4 {
		log.Fatalf("Usage: go run server.go <MongoDB_URI> <Port> <AccountID>")
	}

	mongoURI := os.Args[1]
	port := os.Args[2]
	accountID := os.Args[3]

	startServer(mongoURI, port, accountID)
}
