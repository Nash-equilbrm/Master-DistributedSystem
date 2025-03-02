package main

import (
	"bufio"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"time"
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

func get(client *rpc.Client, req GetRequest) (bool, []byte, error) {
	var reply GetReply
	err := client.Call("LeaderRPC.GetKey", req, &reply)
	if err != nil {
		return false, nil, fmt.Errorf("[FAIL GET]: %s", err)
	}
	if !reply.Success {
		return false, nil, fmt.Errorf("[FAIL GET]: %s", reply.Err)
	}
	return true, reply.Data, nil
}

func set(client *rpc.Client, req SetRequest) (bool, error) {
	var reply SetReply
	err := client.Call("LeaderRPC.Set", req, &reply)
	if err != nil {
		return false, fmt.Errorf("[FAIL SET]: %s", err)
	}
	if !reply.Success {
		return false, fmt.Errorf("[FAIL SET]: %s", reply.Err)
	}
	return true, nil
}

func delete(client *rpc.Client, req DeleteRequest) (bool, error) {
	var reply DeleteReply
	err := client.Call("LeaderRPC.Delete", req, &reply)
	if err != nil {
		return false, fmt.Errorf("[FAIL DELETE]: %v", err)
	}
	if !reply.Success {
		return false, fmt.Errorf("[FAIL SET]: %s", reply.Err)
	}
	return true, nil
}

func getAll(client *rpc.Client, req GetAllRequest) (bool, map[int][]byte, error) {
	var reply GetAllReply
	err := client.Call("LeaderRPC.GetAll", req, &reply)
	if err != nil {
		return false, nil, fmt.Errorf("[FAIL GET]: %s", err)
	}
	if !reply.Success {
		return false, nil, fmt.Errorf("[FAIL GET]: %s", reply.Err)
	}
	return true, reply.Data, nil
}

// For the new test calls
func getInfo(client *rpc.Client, req GetInfoRequest) (bool, string, error) {
	var reply GetInfoReply
	err := client.Call("LeaderRPC.GetInfo", &req, &reply)
	if err != nil {
		return false, "", fmt.Errorf("[FAIL GetInfo]: %v", err)
	}
	if !reply.Success {
		return false, "", fmt.Errorf("[FAIL GetInfo]: %s", reply.Err)
	}
	return true, reply.Info, nil
}

// ---------------------------------------------------------
// Test Routines
// ---------------------------------------------------------

func testSetGetDelete(client *rpc.Client) {
	fmt.Println("== TestSetGetDelete on Leader ==")
	// 1) Set
	ok, _ := set(client, SetRequest{
		Bucket: "User",
		Key:    1,
		Data:   []byte("Do Hoai Nam"),
	})
	fmt.Println(" Set(User,1) with value Do Hoai Nam =>", ok)

	// 2) Get
	ok, data, _ := get(client, GetRequest{
		Bucket: "User",
		Key:    1,
	})
	fmt.Println(" Get(User,1) =>", ok, " - ", string(data))

	// 3) Delete
	ok, _ = delete(client, DeleteRequest{
		Bucket: "User",
		Key:    1,
	})
	fmt.Println(" Delete(User,1) =>", ok)

	// 4) Verify it’s really deleted
	ok, data, _ = get(client, GetRequest{
		Bucket: "User",
		Key:    1,
	})
	fmt.Println(" Get(User,1) after delete =>", ok)
}

func testMassSetAndGet(client *rpc.Client, bucketName string, count int) {
	fmt.Printf("== Mass set/get test for %d items in bucket %q ==\n", count, bucketName)
	startTime := time.Now()

	// Bulk set
	for i := 0; i < count; i++ {
		ok, _ := set(client, SetRequest{
			Bucket: bucketName,
			Key:    i,
			Data:   []byte("Item " + strconv.Itoa(i)),
		})
		if !ok {
			fmt.Printf(" Failed set at i=%d => %s\n", i)
		}
	}

	// Bulk get
	missing := 0
	for i := 0; i < count; i++ {
		ok, _, _ := get(client, GetRequest{
			Bucket: bucketName,
			Key:    i,
		})
		// If val is [FAIL or [ERROR, it indicates an issue
		if !ok {
			missing++
		}
	}

	elapsed := time.Since(startTime)
	fmt.Printf(" Mass set/get done in %v. Missing count=%d\n", elapsed, missing)
	fmt.Println("== Done Mass set/get ==\n")
}

func main() {
	// Hard-coded addresses:
	// - Leader is on :8000
	// - Backup is on :8001
	leaderAddr := "localhost:8000"
	backupAddr := "localhost:8001"

	// 1) Connect to the leader
	leaderClient, err := rpc.Dial("tcp", leaderAddr)
	if err != nil {
		log.Fatalf("Failed to connect leader @ %s: %v", leaderAddr, err)
	}
	fmt.Println("[Connected to leader @", leaderAddr, "]")

	// 2) Basic Set/Get/Delete on Leader
	testSetGetDelete(leaderClient)

	// 3) Mass test
	testMassSetAndGet(leaderClient, "BulkBucket", 20)

	// ---------- NEW TEST CASES FOR GetStoreInfo ----------
	fmt.Println("== Test #1: GetStoreInfo from the leader (should succeed) ==")
	_, storeLeader, _ := getInfo(leaderClient, GetInfoRequest{})
	fmt.Println(" Leader's store info =>", storeLeader)

	fmt.Println("\n== Test #2: GetStoreInfo from backup (should fail or show 'Not the leader') ==")
	backupClient, err := rpc.Dial("tcp", backupAddr)
	if err != nil {
		fmt.Printf("[WARN] Could not connect to backup @ %s: %v\n", backupAddr, err)
		fmt.Println("Skipping backup storeInfo test.")
	} else {
		_, storeBackup, _ := getInfo(backupClient, GetInfoRequest{})
		fmt.Println(" Backup's store info =>", storeBackup)
		backupClient.Close()
	}
	// ------------------------------------------------------

	// 4) Ask user to kill the leader
	fmt.Println("\n[ACTION REQUIRED] Please kill/stop the leader node. Then press ENTER.")
	bufio.NewReader(os.Stdin).ReadBytes('\n')

	// 5) Wait for failover
	fmt.Println("Waiting 5s to let cluster elect new leader on :8000...")
	time.Sleep(5 * time.Second)

	// 6) Connect to the new leader (still :8000)
	newLeaderClient, err := rpc.Dial("tcp", leaderAddr)
	if err != nil {
		log.Fatalf("Failed to connect new leader @ %s: %v", leaderAddr, err)
	}
	fmt.Println("[Connected to new leader @", leaderAddr, "]\n")

	// 7) Verify data is consistent
	fmt.Println("== Checking data from BulkBucket on new leader ==\n")
	_, lastVal, _ := get(newLeaderClient, GetRequest{
		Bucket: "BulkBucket",
		Key:    20},
	)
	fmt.Println(" GetKey(BulkBucket,20) =>", lastVal)

	// 8) Check store info again on new leader
	fmt.Println("\n== Test #3: GetStoreInfo on new leader (post-failover) ==\n")
	_, storeNewLeader, _ := getInfo(newLeaderClient, GetInfoRequest{})
	fmt.Println(" NewLeader store info =>", storeNewLeader)

	// 9) Additional sets/deletes on new leader
	fmt.Println("\n== Testing another SET/DELETE on new leader ==\n")
	setRes, _ := set(newLeaderClient, SetRequest{"FailoverBucket", 1, []byte("DataAfterFailover")})
	fmt.Println(" SetKey(FailoverBucket,1) =>", setRes)

	delRes, _ := delete(newLeaderClient, DeleteRequest{
		Bucket: "FailoverBucket",
		Key:    1,
	})
	fmt.Println(" DeleteKey(FailoverBucket,1) =>", delRes)

	_, postDel, _ := get(newLeaderClient, GetRequest{
		Bucket: "FailoverBucket",
		Key:    1,
	})
	fmt.Println(" GetKey(FailoverBucket,1) =>", postDel)

	newLeaderClient.Close()
	leaderClient.Close()

	fmt.Println("\n=== End of All Tests ===")
}
