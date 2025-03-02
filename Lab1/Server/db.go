package main

import (
	"encoding/json"
	"fmt"
	"github.com/marcelloh/fastdb"
	"github.com/tidwall/gjson"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"strconv"
	"sync"
	"time"
)

type DBService struct {
	db    *fastdb.DB
	mutex *sync.Mutex
}

func NewService() (*DBService, error) {
	db, err := fastdb.Open(":memory:", 100)
	if err != nil {
		log.Fatal(err)
	}

	total := 10 // nr of records to work with

	start := time.Now()
	fillData(db, total)
	log.Printf("created %d records in %s", total, time.Since(start))

	start = time.Now()
	dbRecords, err := db.GetAll("user")
	if err != nil {
		log.Panic(err)
	}

	log.Printf("read %d records in %s", total, time.Since(start))

	sortByUUID(dbRecords)
	return &DBService{
		db:    db,
		mutex: new(sync.Mutex),
	}, nil
}

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

func (l *LeaderRPC) Get(req *GetRequest, reply *GetReply) error {
	dbService := l.node.database

	res, ok := dbService.db.Get(req.Bucket, req.Key)
	if ok {
		reply.Success = ok
		reply.Data = res
	}
	return nil
}

func (l *LeaderRPC) Set(req *SetRequest, reply *SetReply) error {
	dbService := l.node.database
	dbService.mutex.Lock()
	defer dbService.mutex.Unlock()
	err := dbService.db.Set(req.Bucket, req.Key, req.Data)
	if err != nil {
		reply.Success = false
		return err
	}
	reply.Success = true
	l.doReplicate(ReplicateDataRequest{
		BucketName: req.Bucket,
		Key:        req.Key,
		Value:      req.Data,
		Action:     "SET",
	})
	return nil
}

func (l *LeaderRPC) Delete(req *DeleteRequest, reply *DeleteReply) error {
	dbService := l.node.database
	dbService.mutex.Lock()
	defer dbService.mutex.Unlock()
	_, err := dbService.db.Del(req.Bucket, req.Key)
	if err != nil {
		reply.Success = false
		return err
	}
	l.doReplicate(ReplicateDataRequest{
		BucketName: req.Bucket,
		Key:        req.Key,
		Value:      nil,
		Action:     "DELETE",
	})
	reply.Success = true
	return nil
}

func (l *LeaderRPC) GetAll(req *GetAllRequest, reply *GetAllReply) error {
	dbService := l.node.database

	data, err := dbService.db.GetAll(req.Bucket)
	if err != nil {
		reply.Success = false
		return err
	}
	reply.Success = true
	reply.Data = data
	return nil
}

func (l *LeaderRPC) GetInfo(req *GetInfoRequest, reply *GetInfoReply) error {
	dbService := l.node.database

	info := dbService.db.Info()
	if info != "" && len(info) > 0 {
		reply.Success = true
		reply.Info = info
		return nil
	}
	reply.Success = false
	return nil
}

// FastDB
type user struct {
	ID    int
	UUID  string
	Email string
}

type record struct {
	SortField any
	Data      []byte
}

func sortByUUID(dbRecords map[int][]byte) {
	start := time.Now()
	count := 0
	keys := make([]record, len(dbRecords))

	for key := range dbRecords {
		jsonRecord := string(dbRecords[key])
		value := gjson.Get(jsonRecord, "UUID").Str + strconv.Itoa(key)
		keys[count] = record{SortField: value, Data: dbRecords[key]}
		count++
	}

	sort.Slice(keys, func(i, j int) bool {
		return keys[i].SortField.(string) < keys[j].SortField.(string)
	})

	log.Printf("sort %d records by UUID in %s", count, time.Since(start))

	for key, value := range keys {
		if key >= 15 {
			break
		}

		fmt.Printf("value : %v\n", string(value.Data))
	}
}

func fillData(store *fastdb.DB, total int) {
	user := &user{
		ID:    1,
		UUID:  "UUIDtext_",
		Email: "test@example.com",
	}

	for i := 1; i <= total; i++ {
		user.ID = i
		user.UUID = "UUIDtext_" + generateRandomString(8) + strconv.Itoa(user.ID)

		userData, err := json.Marshal(user)
		if err != nil {
			log.Fatal(err)
		}

		err = store.Set("user", user.ID, userData)
		if err != nil {
			log.Fatal(err)
		}
	}
}

// generateRandomString creates a random string of the specified length
func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	b := make([]byte, length)

	_, err := rand.Read(b)
	if err != nil {
		log.Fatal(err)
	}

	for i := range b {
		b[i] = charset[int(b[i])%len(charset)]
	}

	return string(b)
}

func startDbService() (*DBService, error) {
	// Create a new RPC server
	service, err := NewService()
	if err != nil {
		log.Fatal("Error creating service:", err)
		return nil, err
	}
	// Register RPC server
	err = rpc.Register(service)
	if err != nil {
		log.Fatal("error registering:", err)
		return nil, err
	}
	rpc.HandleHTTP()
	// Listen for requests on port 1234
	l, e := net.Listen("tcp", ":2233")
	if e != nil {
		log.Fatal("listen error:", e)
		return nil, err
	}
	err = http.Serve(l, nil)
	if err != nil {
		log.Fatal("http serve error:", err)
		return nil, err
	}
	return service, nil
}
