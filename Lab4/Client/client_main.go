package main

import (
	"fmt"
	"github.com/google/uuid"
	"log"
	"math/rand"
	"net/rpc"
	"time"
)

// Structs dùng chung giữa Client, LoadBalancer và Server
type GetRequest struct {
	Bucket string
	Key    int
}
type GetReply struct {
	Success bool
	Err     error
	Data    []byte
}

type SetRequest struct {
	Bucket string
	Key    int
	Data   []byte
}
type SetReply struct {
	Success bool
	Err     error
}

type DeleteRequest struct {
	Bucket string
	Key    int
}
type DeleteReply struct {
	Success bool
	Err     error
}

type GetAllRequest struct {
	Bucket string
}
type GetAllReply struct {
	Success bool
	Data    map[int][]byte
	Err     error
}

type RemoveServerRequest struct {
	Address string
}

type RemoveServerReply struct {
	Success bool
	Message string
}

// LoadBalancerClient giúp giao tiếp với LoadBalancer
type LoadBalancerClient struct {
	client *rpc.Client
}

// NewLoadBalancerClient tạo kết nối với LoadBalancer
func NewLoadBalancerClient(address string) (*LoadBalancerClient, error) {
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		return nil, err
	}
	return &LoadBalancerClient{client: client}, nil
}

// Set gửi dữ liệu đến LoadBalancer
func (lb *LoadBalancerClient) Set(bucket string, key int, value []byte) error {
	log.Printf("Client sending Set request - Bucket: %s, Key: %d", bucket, key)

	req := &SetRequest{Bucket: bucket, Key: key, Data: value}
	reply := &SetReply{}
	err := lb.client.Call("LoadBalancer.Set", req, reply)

	if err != nil {
		log.Printf("Client failed to send Set request: %v", err)
		return err
	}

	log.Printf("Client received response: Success = %v", reply.Success)
	return nil
}

// Get lấy dữ liệu từ LoadBalancer
func (lb *LoadBalancerClient) Get(bucket string, key int) ([]byte, error) {
	req := &GetRequest{Bucket: bucket, Key: key}
	reply := &GetReply{}
	err := lb.client.Call("LoadBalancer.Get", req, reply)
	if err != nil {
		return nil, err
	}
	return reply.Data, nil
}

func (lb *LoadBalancerClient) SetMultipleData(randomId bool) {

	for i := 1; i <= 10; i++ {
		var id int
		if randomId {
			id = rand.Intn(1000) + 1 // ID từ 1 đến 1000
		} else {
			id = i
		}
		uuidStr := uuid.New().String()
		email := fmt.Sprintf("user%d_%d@example.com", id, id)

		data := fmt.Sprintf(`{"ID":%d,"UUID":"%s","Email":"%s"}`, id, uuidStr, email)
		log.Printf("Client sending Set request - Bucket: user, Key: %d", i)

		req := &SetRequest{Bucket: "user", Key: id, Data: []byte(data)}
		reply := &SetReply{}
		err := lb.client.Call("LoadBalancer.Set", req, reply)

		if err != nil {
			log.Printf("Client failed to send Set request: %v", err)
			continue
		}

		log.Printf("Client received response for Key %d: Success = %v", i, reply.Success)

		// Random sleep từ 200ms đến 800ms để mô phỏng request không đồng đều
		time.Sleep(time.Duration(rand.Intn(600)+200) * time.Millisecond)
	}
}

func (lb *LoadBalancerClient) GetMultipleData() {
	for i := 1; i <= 10; i++ {
		log.Printf("Client sending Get request - Bucket: user, Key: %d", i)

		req := &GetRequest{Bucket: "user", Key: i}
		reply := &GetReply{}
		err := lb.client.Call("LoadBalancer.Get", req, reply)

		if err != nil {
			log.Printf("Client failed to get data for Key %d: %v", i, err)
			continue
		}

		if reply.Success {
			log.Printf("Client received response for Key %d: %s", i, string(reply.Data))
		} else {
			log.Printf("Client received error for Key %d: %v", i, reply.Err)
		}

		time.Sleep(time.Millisecond * 500)
	}
}

// RemoveServer yêu cầu LoadBalancer loại bỏ một server
func (lb *LoadBalancerClient) RemoveServer(serverID string) error {
	log.Printf("Client sending RemoveServer request - ServerID: %s", serverID)

	req := &RemoveServerRequest{Address: serverID}
	reply := &RemoveServerReply{}
	err := lb.client.Call("LoadBalancer.RemoveServer", req, reply)

	if err != nil {
		log.Printf("Client failed to send RemoveServer request: %v", err)
		return err
	}

	log.Printf("Client received response: Success = %v", reply.Success)
	return nil
}

func main() {
	// Kết nối với LoadBalancer
	lbClient, err := NewLoadBalancerClient("localhost:9000")
	if err != nil {
		log.Fatalf("Error connecting to LoadBalancer: %v", err)
	}
	defer lbClient.client.Close()

	// Uncomment to set 10 new data entries
	//lbClient.SetMultipleData(false)
	//lbClient.SetMultipleData(true)

	// Uncomment to get 10 data entries
	//lbClient.GetMultipleData()

	// Uncomment to request remove server
	serverToRemoveID := "localhost:2"
	lbClient.RemoveServer(serverToRemoveID)
}
