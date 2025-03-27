package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/marcelloh/fastdb"
)

type Node struct {
	id       int
	peerIds  []int
	leaderId int

	electionMutex     sync.Mutex
	isElectionRunning bool

	internalListener net.Listener
	internalServer   *rpc.Server

	leaderListener net.Listener
	leaderServer   *rpc.Server

	database *Database
}

type Database struct {
	db    *fastdb.DB
	mutex sync.Mutex
}

type InternalRPC struct {
	node *Node
}

type LeaderRPC struct {
	node *Node
}

func main() {
	nodeID := (flag.Int("id", 1, "Node ID"))
	peersStr := flag.String("peers", "", "Comma-separated list of peer IDs")
	flag.Parse()

	if *peersStr == "" {
		log.Fatal("Peers must be specified via -peers")
	}
	var peerIDs []int
	for _, p := range strings.Split(*peersStr, ",") {
		pid, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil {
			log.Fatalf("Invalid peer ID: %s", p)
		}
		if pid != *nodeID {
			peerIDs = append(peerIDs, pid)
		}
	}

	store, err := fastdb.Open(":memory:", 100)
	if err != nil {
		message := fmt.Sprintf("[Node %d] Failed to open db", nodeID)
		log.Fatal(message)
	}
	sharedDataBase := Database{db: store}
	node := &Node{
		id:       *nodeID,
		peerIds:  peerIDs,
		leaderId: -1,

		isElectionRunning: false,

		database: &sharedDataBase,
	}

	node.startInternalServer(&sharedDataBase)

	time.Sleep(5 * time.Second)

	node.startHeartbeatRoutine()

	select {}
}
