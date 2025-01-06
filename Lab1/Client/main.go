package main

import (
	"fmt"
	"log"
	"net/rpc"
)

// Scheme
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

func main() {
	// Connect to the RPC server
	client, err := rpc.DialHTTP("tcp", "localhost:2233")
	if err != nil {
		log.Fatalf("Error connecting to RPC server: %v", err)
	}
	defer client.Close()

	// Test Set API
	setRequest := &SetRequest{
		Bucket: "user",
		Key:    1,
		Data:   []byte(`{"ID":1,"UUID":"UUIDtext_12345678","Email":"dohoainam17052001@gmail.com"}`),
	}
	setReply := &SetReply{}
	err = client.Call("Service.Set", setRequest, setReply)
	if err != nil {
		log.Fatalf("Error calling Set: %v", err)
	}
	fmt.Printf("Set Success: %v\n", setReply.Success)

	// Test Get API
	getRequest := &GetRequest{
		Bucket: "user",
		Key:    1,
	}
	getReply := &GetReply{}
	err = client.Call("Service.Get", getRequest, getReply)
	if err != nil {
		log.Fatalf("Error calling Get: %v", err)
	}
	fmt.Printf("Get Success: %v, Data: %s\n", getReply.Success, string(getReply.Data))

	// Test GetAll API
	getAllRequest := &GetAllRequest{
		Bucket: "user",
	}
	getAllReply := &GetAllReply{}
	err = client.Call("Service.GetAll", getAllRequest, getAllReply)
	if err != nil {
		log.Fatalf("Error calling GetAll: %v", err)
	}
	fmt.Printf("GetAll Success: %v, Data:\n", getAllReply.Success)
	for r := range getAllReply.Data {
		fmt.Printf("Key: %d, Data: %s\n", r, string(getAllReply.Data[r]))
	}

	// Test Delete API
	deleteRequest := &DeleteRequest{
		Bucket: "user",
		Key:    1,
	}
	deleteReply := &DeleteReply{}
	err = client.Call("Service.Delete", deleteRequest, deleteReply)
	if err != nil {
		log.Fatalf("Error calling Delete: %v", err)
	}
	fmt.Printf("Delete Success: %v\n", deleteReply.Success)

	// Test GetAll API
	getAllRequest = &GetAllRequest{
		Bucket: "user",
	}
	getAllReply = &GetAllReply{}
	err = client.Call("Service.GetAll", getAllRequest, getAllReply)
	if err != nil {
		log.Fatalf("Error calling GetAll: %v", err)
	}
	fmt.Printf("GetAll Success: %v, Data:\n", getAllReply.Success)
	for r := range getAllReply.Data {
		fmt.Printf("Key: %d, Data: %s\n", r, string(getAllReply.Data[r]))
	}

	// Test GetInfo API
	getInfoRequest := &GetInfoRequest{}
	getInfoReply := &GetInfoReply{}
	err = client.Call("Service.GetInfo", getInfoRequest, getInfoReply)
	if err != nil {
		log.Fatalf("Error calling GetInfo: %v", err)
	}
	fmt.Printf("GetInfo Success: %v, Info: %s\n", getInfoReply.Success, getInfoReply.Info)
}
