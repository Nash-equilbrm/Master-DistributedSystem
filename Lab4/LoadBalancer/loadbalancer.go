package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
)

// ==== Structs Request & Reply ====
type RegisterServerRequest struct {
	Address string
}
type RegisterServerReply struct {
	Success bool
	Message string
}

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

type GetInfoRequest struct{}
type GetInfoReply struct {
	Success bool
	Info    string
	Err     error
}

// ==== LoadBalancer ====
type LoadBalancer struct {
	hashRing *Map
	servers  map[string]*rpc.Client
	//nodes    []string
	mutex sync.RWMutex
}

// ==== Khởi tạo LoadBalancer ====
func NewLoadBalancer() *LoadBalancer {
	return &LoadBalancer{
		hashRing: NewConsistentHash(10, nil),
		servers:  make(map[string]*rpc.Client),
		//nodes:    []string{},
	}
}

// ==== Đăng ký Server mới ====
func (lb *LoadBalancer) RegisterServer(req *RegisterServerRequest, reply *RegisterServerReply) error {
	lb.mutex.Lock()
	defer lb.mutex.Unlock()

	if _, exists := lb.servers[req.Address]; exists {
		reply.Success = false
		reply.Message = fmt.Sprintf("Server %s is already registered", req.Address)
		log.Println(reply.Message)
		return nil
	}

	client, err := rpc.DialHTTP("tcp", req.Address)
	if err != nil {
		reply.Success = false
		reply.Message = fmt.Sprintf("Failed to connect to server %s: %v", req.Address, err)
		log.Println(reply.Message)
		return err
	}

	// Thêm vào hash ring
	lb.hashRing.Add(req.Address)
	lb.servers[req.Address] = client

	reply.Success = true
	reply.Message = fmt.Sprintf("Added server: %s", req.Address)

	log.Printf("NewConsistentHash server registered: %s", req.Address)
	log.Println("Current servers in LoadBalancer:")
	for addr, _ := range lb.servers {
		log.Printf("   - %s", addr)
	}

	// Thực hiện rebalance dữ liệu
	lb.rebalanceData(req.Address)
	log.Println("--------------------------------")
	return nil
}

// ==== Rebalance Data ====
func (lb *LoadBalancer) rebalanceData(newServerAddr string) {
	log.Printf("Rebalancing data for new server: %s", newServerAddr)

	// Nếu chỉ có 1 node, không cần rebalance
	if len(lb.servers) < 2 {
		log.Printf("Not enough servers for rebalancing")
		return
	}

	// Tìm node đứng liền sau trong vòng băm
	nextServerAddr := lb.hashRing.Get(newServerAddr)

	log.Printf("Data will be moved from next server: %s", nextServerAddr)

	// Kiểm tra kết nối
	nextServer, exists := lb.servers[nextServerAddr]
	if !exists {
		log.Printf("next server %s is not connected, skipping migration", nextServerAddr)
		return
	}

	// Lấy toàn bộ dữ liệu từ node sau
	getAllReq := &GetAllRequest{Bucket: "user"}
	getAllReply := &GetAllReply{}
	err := nextServer.Call("Service.GetAll", getAllReq, getAllReply)
	if err != nil {
		log.Printf("Failed to get data from %s: %v", nextServerAddr, err)
		return
	}

	// Di chuyển dữ liệu sang node mới
	for key, value := range getAllReply.Data {
		newServer, _ := lb.GetServerForKey(fmt.Sprintf("user_%d", key))
		if newServer == newServerAddr {
			lb.migrateData(newServerAddr, nextServerAddr, key, value)
		}
	}
}

func (lb *LoadBalancer) migrateData(newServerAddr string, oldServerAddr string, key int, data []byte) {
	log.Printf("Migrating key %d to new server %s", key, newServerAddr)

	// Gửi Set request tới server mới
	newServer := lb.servers[newServerAddr]
	setReq := &SetRequest{Bucket: "user", Key: key, Data: data}
	setReply := &SetReply{}

	err := newServer.Call("Service.Set", setReq, setReply)
	if err != nil || !setReply.Success {
		log.Printf("Failed to migrate key %d to %s: %v", key, newServerAddr, err)
		return
	}
	log.Printf("Successfully migrated key %d to server %s", key, newServerAddr)

	// Xóa dữ liệu trên server cũ sau khi di chuyển thành công
	log.Printf("Deleting key %d from old server %s", key, oldServerAddr)

	oldServer := lb.servers[oldServerAddr]
	delReq := &DeleteRequest{Bucket: "user", Key: key}
	delReply := &DeleteReply{}

	err = oldServer.Call("Service.Delete", delReq, delReply)
	if err != nil || !delReply.Success {
		log.Printf("Failed to delete key %d from old server %s: %v", key, oldServerAddr, err)
	} else {
		log.Printf("Successfully deleted key %d from old server %s", key, oldServerAddr)
	}
}

// ==== Xóa Server ====
func (lb *LoadBalancer) RemoveServer(address string) {
	lb.mutex.Lock()
	defer lb.mutex.Unlock()

	// Kiểm tra server có tồn tại không
	if _, exists := lb.servers[address]; !exists {
		log.Printf("Server %s not found, skipping removal", address)
		return
	}
	err := lb.servers[address].Close()
	if err != nil {
		return
	}
	// Xóa khỏi danh sách servers
	delete(lb.servers, address)

	// Cập nhật danh sách nodes
	lb.hashRing = NewConsistentHash(10, nil)

	for nodeAddress, _ := range lb.servers {
		if address != nodeAddress {
			lb.hashRing.Add(nodeAddress)
		}
	}

	log.Printf("Removed server: %s", address)
}

// ==== Lấy Server theo key ====
func (lb *LoadBalancer) GetServerForKey(key string) (string, error) {
	//lb.mutex.RLock()
	//defer lb.mutex.RUnlock()

	server := lb.hashRing.Get(key)
	if server == "" {
		return "", fmt.Errorf("no available servers")
	}
	log.Printf("%s will be moved/stayed at %s", key, server)
	return server, nil
}

// ==== Set Request ====
func (lb *LoadBalancer) Set(req *SetRequest, reply *SetReply) error {
	log.Printf("LoadBalancer received Set request for key: %d, bucket: %s", req.Key, req.Bucket)

	server, err := lb.GetServerForKey(fmt.Sprintf("%s_%d", req.Bucket, req.Key))
	if err != nil {
		log.Printf("No server found for key: %d, error: %v", req.Key, err)
		return err
	}
	log.Printf("LoadBalancer selected server %s for key %d", server, req.Key)

	client, exists := lb.servers[server]
	if !exists {
		log.Printf("Server %s not found in lb.servers map", server)
		return fmt.Errorf("server %s not found", server)
	}

	err = client.Call("Service.Set", req, reply)
	if err != nil {
		log.Printf("Failed to forward Set request to server %s: %v", server, err)
		return err
	}

	log.Printf("LoadBalancer received response from server %s: Success = %v", server, reply.Success)
	return nil
}

// ==== Get Request ====
func (lb *LoadBalancer) Get(req *GetRequest, reply *GetReply) error {
	log.Printf("LoadBalancer received Get request for key: %d, bucket: %s", req.Key, req.Bucket)

	// Tìm server thích hợp bằng consistent hashing
	server, err := lb.GetServerForKey(fmt.Sprintf("%s_%d", req.Bucket, req.Key))
	if err != nil {
		log.Printf("No server found for key: %d, error: %v", req.Key, err)
		return err
	}
	log.Printf("LoadBalancer selected server %s for key %d", server, req.Key)

	// Kiểm tra server có tồn tại không
	client, exists := lb.servers[server]
	if !exists {
		log.Printf("Server %s not found in lb.servers map", server)
		return fmt.Errorf("server %s not found", server)
	}

	// Gửi request Get tới server node
	err = client.Call("Service.Get", req, reply)
	if err != nil {
		log.Printf("Failed to forward Get request to server %s: %v", server, err)
		return err
	}

	// Log kết quả từ server
	if reply.Success {
		log.Printf("LoadBalancer received response from server %s: Key %d found", server, req.Key)
	} else {
		log.Printf("LoadBalancer received response from server %s: Key %d not found", server, req.Key)
	}

	return nil
}

// ==== Chạy LoadBalancer ====
func main() {
	lb := NewLoadBalancer()
	rpc.Register(lb)
	rpc.HandleHTTP()
	listener, _ := net.Listen("tcp", ":9000")
	log.Println("LoadBalancer listening on port 9000...")
	http.Serve(listener, nil)
}
