package main

import (
	"flag"
	"fmt"
	"github.com/marcelloh/fastdb"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ---------------------- Main ----------------------
func main() {
	nodeID := flag.Int("id", 1, "Node ID")
	nodeIDsStr := flag.String("peers", "", "Semicolon-separated list of peer IDs")
	flag.Parse()

	if *nodeIDsStr == "" {
		log.Fatal("Peers must be specified via -peers")
	}

	var peerIDs []int
	for _, p := range strings.Split(*nodeIDsStr, ",") {
		pid, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil {
			log.Fatalf("Invalid peer ID: %s", p)
		}
		if pid != *nodeID {
			peerIDs = append(peerIDs, pid)
		}
	}

	db, err := fastdb.Open(":memory:", 100)
	if err != nil {
		message := fmt.Sprintf("[Node %d] Failed to start database", nodeID)
		log.Fatal(message)
	}

	sharedDbService := DBService{db: db, mutex: new(sync.Mutex)}
	node := &Node{
		id:       *nodeID,
		peerIds:  peerIDs,
		leaderId: -1,

		isElectionGoging: false,

		database: &sharedDbService,
	}

	node.startInternalServer()

	time.Sleep(5 * time.Second)

	node.startPingRoutine()

	// Prevent exiting
	select {}
}
