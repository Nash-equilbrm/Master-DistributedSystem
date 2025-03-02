package main

import (
	"fmt"
	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash/v2"
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

// ==== ServerNode (cần để triển khai consistent.Member) ====
type ServerNode struct {
	Address string
}

// Cung cấp phương thức String() để ServerNode phù hợp với consistent.Member
func (s ServerNode) String() string {
	return s.Address
}

// ==== LoadBalancer ====
type LoadBalancer struct {
	hashRing *consistent.Consistent
	servers  map[string]*rpc.Client
	mutex    sync.RWMutex
}

// ==== Khởi tạo LoadBalancer ====
func NewLoadBalancer() *LoadBalancer {
	cfg := consistent.Config{
		Hasher:            hasher{},
		PartitionCount:    71,
		ReplicationFactor: 3,
		Load:              1.25,
	}
	return &LoadBalancer{
		hashRing: consistent.New(nil, cfg),
		servers:  make(map[string]*rpc.Client),
	}
}

// ==== Custom Hasher ====
type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

// ==== Server tự động đăng ký vào LoadBalancer ====
func (lb *LoadBalancer) RegisterServer(req *RegisterServerRequest, reply *RegisterServerReply) error {
	lb.mutex.Lock()
	defer lb.mutex.Unlock()

	// Kiểm tra nếu server đã tồn tại
	if _, exists := lb.servers[req.Address]; exists {
		reply.Success = false
		reply.Message = fmt.Sprintf("Server %s is already registered", req.Address)
		log.Println(reply.Message)
		return nil
	}

	// Kết nối đến server mới
	client, err := rpc.DialHTTP("tcp", req.Address)
	if err != nil {
		reply.Success = false
		reply.Message = fmt.Sprintf("Failed to connect to server %s: %v", req.Address, err)
		log.Println(reply.Message)
		return err
	}

	// Thêm server vào LoadBalancer
	node := ServerNode{Address: req.Address}
	lb.hashRing.Add(node)
	lb.servers[req.Address] = client
	reply.Success = true
	reply.Message = fmt.Sprintf("Added server: %s", req.Address)

	// Log server mới đã đăng ký
	log.Printf("New server registered: %s", req.Address)
	log.Println("Current servers in LoadBalancer:")
	for addr := range lb.servers {
		log.Printf("   - %s", addr)
	}
	lb.rebalanceData(node)

	log.Println("--------------------------------")

	return nil
}

// ==== Tìm server kế tiếp trên vòng băm và chuyển dữ liệu sang server mới ====
func (lb *LoadBalancer) rebalanceData(newNode ServerNode) {
	log.Printf("Rebalancing data for new server: %s", newNode.Address)

	// Lấy danh sách tất cả các server trên vòng băm
	members := lb.hashRing.GetMembers()
	if len(members) < 2 {
		log.Printf("Not enough servers for rebalancing")
		return
	}

	// Tìm vị trí của server mới trong danh sách
	var newServerIndex int
	for i, member := range members {
		if member.String() == newNode.Address {
			newServerIndex = i
			break
		}
	}

	// Xác định server kế tiếp (theo vòng tròn)
	nextServerIndex := (newServerIndex + 1) % len(members)
	nextServerAddr := members[nextServerIndex].String()

	log.Printf("Data will be moved from next server: %s", nextServerAddr)

	// Kiểm tra nếu server kế tiếp có trong danh sách kết nối của LoadBalancer
	nextServer, exists := lb.servers[nextServerAddr]
	if !exists {
		log.Printf("Next server %s is not connected, skipping data migration", nextServerAddr)
		return
	}

	// Lấy tất cả dữ liệu từ server kế tiếp
	getAllReq := &GetAllRequest{Bucket: "user"}
	getAllReply := &GetAllReply{}
	err := nextServer.Call("Service.GetAll", getAllReq, getAllReply)
	if err != nil {
		log.Printf("Failed to get data from next server %s: %v (possibly empty database)", nextServerAddr, err)
		return
	}

	// Kiểm tra nếu server kế tiếp có dữ liệu hay không
	if len(getAllReply.Data) == 0 {
		log.Printf("Next server %s has no data, no migration needed", nextServerAddr)
		return
	}

	// Kiểm tra key nào cần chuyển sang server mới
	for key, value := range getAllReply.Data {
		newServer, _ := lb.GetServerForKey(fmt.Sprintf("user_%d", key))
		if newServer == newNode.Address {
			// Nếu key thuộc về server mới, gửi dữ liệu từ server kế tiếp sang server mới
			lb.migrateData(newNode.Address, nextServerAddr, key, value)
		}
	}
}

// ==== Chuyển dữ liệu từ server cũ sang server mới và xóa dữ liệu cũ ====
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

// ==== Xóa server khỏi LoadBalancer ====
func (lb *LoadBalancer) RemoveServer(address string) {
	lb.mutex.Lock()
	defer lb.mutex.Unlock()

	lb.hashRing.Remove(address)

	if client, exists := lb.servers[address]; exists {
		client.Close()
		delete(lb.servers, address)
		log.Printf("Removed server: %s", address)

		// Log danh sách server sau khi xóa
		log.Println("Current servers after removal:")
		for addr := range lb.servers {
			log.Printf("   - %s", addr)
		}
		log.Println("--------------------------------")
	}
}

// ==== Lấy server phù hợp cho key ====
func (lb *LoadBalancer) GetServerForKey(key string) (string, error) {
	log.Printf("GetServerForKey: %s", key)
	members := lb.hashRing.GetMembers()
	if len(members) == 0 {
		log.Printf("Hash ring is empty! No servers available.")
		return "", fmt.Errorf("no available servers")
	}
	//lb.mutex.RLock()
	//defer lb.mutex.RUnlock()

	node := lb.hashRing.LocateKey([]byte(key))

	if node == nil {
		log.Printf("LocateKey() failed for key: %s", key)
		return "", fmt.Errorf("failed to locate server for key: %s", key)
	}

	return node.String(), nil
}

// ==== Chuyển tiếp request Set ====
func (lb *LoadBalancer) Set(req *SetRequest, reply *SetReply) error {
	log.Printf("LoadBalancer received Set request for key: %d, bucket: %s", req.Key, req.Bucket)

	server, err := lb.GetServerForKey(fmt.Sprintf("%s:%d", req.Bucket, req.Key))
	if err != nil {
		log.Printf("No server found for key: %d", req.Key)
		return err
	}
	log.Printf("LoadBalancer selected server %s for key %d", server, req.Key)

	client := lb.servers[server]
	err = client.Call("Service.Set", req, reply)
	if err != nil {
		log.Printf("Failed to forward Set request to server %s: %v", server, err)
		return err
	}

	log.Printf("LoadBalancer received response from server %s: Success = %v", server, reply.Success)
	return nil
}

// ==== Chuyển tiếp request Get ====
func (lb *LoadBalancer) Get(req *GetRequest, reply *GetReply) error {
	server, err := lb.GetServerForKey(fmt.Sprintf("%s:%d", req.Bucket, req.Key))
	if err != nil {
		return err
	}
	client := lb.servers[server]
	return client.Call("Service.Get", req, reply)
}

// ==== Chuyển tiếp request Delete ====
func (lb *LoadBalancer) Delete(req *DeleteRequest, reply *DeleteReply) error {
	server, err := lb.GetServerForKey(fmt.Sprintf("%s:%d", req.Bucket, req.Key))
	if err != nil {
		return err
	}
	client := lb.servers[server]
	return client.Call("Service.Delete", req, reply)
}

// ==== Chuyển tiếp request GetAll ====
func (lb *LoadBalancer) GetAll(req *GetAllRequest, reply *GetAllReply) error {
	server, err := lb.GetServerForKey(req.Bucket)
	if err != nil {
		return err
	}
	client := lb.servers[server]
	return client.Call("Service.GetAll", req, reply)
}

// ==== Chuyển tiếp request GetInfo ====
func (lb *LoadBalancer) GetInfo(req *GetInfoRequest, reply *GetInfoReply) error {
	lb.mutex.Lock()
	defer lb.mutex.Unlock()

	for _, client := range lb.servers {
		err := client.Call("Service.GetInfo", req, reply)
		if err == nil {
			return nil
		}
	}
	return fmt.Errorf("no available servers")
}

// ==== Chạy LoadBalancer ====
func main() {
	lb := NewLoadBalancer()
	err := rpc.Register(lb)
	if err != nil {
		log.Fatal("Failed to register LoadBalancer:", err)
	}
	rpc.HandleHTTP()

	listener, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatal("Listen error:", err)
	}
	log.Println("LoadBalancer listening on port 9000...")
	err = http.Serve(listener, nil)
	if err != nil {
		log.Fatal("HTTP serve error:", err)
	}
}
