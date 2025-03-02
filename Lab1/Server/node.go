package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
)

type Node struct {
	id       int
	peerIds  []int
	leaderId int

	electionMutex    sync.Mutex
	isElectionGoging bool

	internalListener net.Listener
	internalServer   *rpc.Server

	leaderListener net.Listener
	leaderServer   *rpc.Server

	database *DBService
}

type InternalRPC struct {
	node *Node
}

type LeaderRPC struct {
	node *Node
}

func (n *Node) _callToPeer(targetNodeId int, port int, method string, args interface{}, reply interface{}) error {
	address := fmt.Sprintf("localhost:%d", port)
	client, err := rpc.Dial("tcp", address)
	log.Printf("[Node %d] Calling API %s", n.id, method)
	if err != nil {
		log.Printf("[Node %d] Failed connect to peer %d at %d: %v\n",
			n.id, targetNodeId, port, err)
		return err
	}
	defer func(client *rpc.Client) {
		err := client.Close()
		if err != nil {

		}
	}(client)
	return client.Call(method, args, reply)

}

func (n *Node) callToPeer(peerId int, method string, args interface{}, reply interface{}) error {
	var port int = 8000 + peerId
	return n._callToPeer(peerId, port, method, args, reply)
}

func (n *Node) callToLeader(leaderId int, method string, args interface{}, reply interface{}) error {
	var port = 8000
	return n._callToPeer(leaderId, port, method, args, reply)
}

func (n *Node) startInternalServer() {
	n.internalServer = rpc.NewServer()

	err := n.internalServer.Register(&InternalRPC{
		node: n,
	})
	if err != nil {
		log.Fatalf("[Node %d] Failed to register", n.id)
		return
	}

	defaultPort := 8000
	addr := fmt.Sprintf(":%d", defaultPort+n.id)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("[Node %d] Cannot listen on %s: %v", n.id, addr, err)
	}

	n.internalListener = l
	log.Printf("[Node %d] Internal server listening on %s", n.id, addr)
	go func() {
		for {
			conn, _ := l.Accept()
			go n.internalServer.ServeConn(conn)
		}
	}()
}

func (n *Node) startLeaderServer() {
	n.leaderServer = rpc.NewServer()
	err := n.leaderServer.Register(&LeaderRPC{
		node: n,
	})
	if err != nil {
		log.Fatalf(err.Error())
	}

	l, err := net.Listen("tcp", ":8000")
	if err != nil {
		log.Fatalf("[Node %d] Cannot listen on %s: %v", n.id, "8000", err)
	}
	n.leaderListener = l
	log.Printf("[Node %d] Leader server is listening on %s", n.id, "8000")
	go func() {
		// Continuous loop
		for {
			conn, err := l.Accept()
			if err != nil {
				log.Printf("[Node %d] Leader listener closed: %v", n.id, err)
				return
			}
			go n.leaderServer.ServeConn(conn)
		}
	}()
}
