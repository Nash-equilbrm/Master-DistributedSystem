package main

import (
	"fmt"
	"log"
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

func (lb *LoadBalancerClient) SetMultipleData() {
	for i := 1; i <= 10; i++ {
		data := fmt.Sprintf(`{"ID":%d,"UUID":"uuid_%d","Email":"user%d@example.com"}`, i, i, i)
		log.Printf("Client sending Set request - Bucket: user, Key: %d", i)

		req := &SetRequest{Bucket: "user", Key: i, Data: []byte(data)}
		reply := &SetReply{}
		err := lb.client.Call("LoadBalancer.Set", req, reply)

		if err != nil {
			log.Printf("Client failed to send Set request: %v", err)
			continue
		}

		log.Printf("Client received response for Key %d: Success = %v", i, reply.Success)
		time.Sleep(time.Millisecond * 500) // Giãn cách để dễ theo dõi log
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

func main() {
	// Kết nối với LoadBalancer
	lbClient, err := NewLoadBalancerClient("localhost:9000")
	if err != nil {
		log.Fatalf("Error connecting to LoadBalancer: %v", err)
	}
	defer lbClient.client.Close()

	// Uncomment to set 10 new data entries
	//lbClient.SetMultipleData()

	// Uncomment to get 10 data entries
	lbClient.GetMultipleData()
}
