package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"time"
)

type ElectionReq struct {
	SenderID int
}

type ElectionRes struct {
	Ack bool
}

type NotifyLeaderReq struct {
	LeaderId int
}

type NotifyLeaderRes struct {
	Ack bool
}

func (node *Node) callToFollower(peerId int, method string, args interface{}, reply interface{}) error {
	var port int = 8000 + peerId
	return node.callToPeer(peerId, port, method, args, reply)
}

func (node *Node) callToLeader(leaderId int, method string, args interface{}, reply interface{}) error {
	var port int = 8000
	return node.callToPeer(leaderId, port, method, args, reply)
}
func (node *Node) callToPeer(targetNodeId int, port int, method string, args interface{}, reply interface{}) error {
	address := fmt.Sprintf("localhost:%d", port)
	client, err := rpc.Dial("tcp", address)
	log.Printf("[Node %d] Calling method %s", node.id, method)
	if err != nil {
		log.Printf("[Node %d] Could not connect to peer %d at %d: %v\n",
			node.id, targetNodeId, port, err)
		return err
	}
	defer client.Close()
	return client.Call(method, args, reply)

}

func (node *Node) startElection() {
	node.electionMutex.Lock()
	if node.isElectionRunning {
		node.electionMutex.Unlock()
		return
	}
	node.isElectionRunning = true
	node.electionMutex.Unlock()

	higherIds := []int{}
	for _, peerId := range node.peerIds {
		if peerId > node.id {
			higherIds = append(higherIds, peerId)
		}
	}
	var isFoundHigherActiveNode = false
	for _, nextId := range higherIds {
		log.Printf("[Node %d] Call Election to node %d ", node.id, nextId)
		var electionReply ElectionRes
		err := node.callToFollower(
			nextId,
			"InternalRPC.Election",
			ElectionReq{node.id},
			&electionReply,
		)
		if err != nil {
			log.Println("Has error while trying to get key", err)
		} else if electionReply.Ack {
			// Have another higher node. Abort
			isFoundHigherActiveNode = true
			break
		}
	}

	if !isFoundHigherActiveNode {
		node.isElectionRunning = false
		// promote to leader and notified all others node
		node.becomeLeader()
	}
}

func (rpcNode *InternalRPC) Election(args ElectionReq, reply *ElectionRes) error {
	// When you receive this. response with OK then continue to call Election to the next node
	reply.Ack = true

	// Check if this node is start election process or not. If not, start one
	if !rpcNode.node.isElectionRunning {
		rpcNode.node.startElection()
	}
	return nil
}

type StepDownReq struct {
	SenderID int
}

type StepDownRes struct {
	Ok bool
}

func (rpcNode *LeaderRPC) StepDown(args StepDownReq, reply *StepDownRes) error {
	if rpcNode.node.id != rpcNode.node.leaderId {
		reply.Ok = true
		return nil
	}
	if rpcNode.node.id > args.SenderID {
		reply.Ok = false
	} else {
		rpcNode.node.leaderListener.Close()
		reply.Ok = true
	}
	return nil
}

func (rpcNode *InternalRPC) NotifyLeader(args NotifyLeaderReq, reply *NotifyLeaderRes) error {
	newLeader := args.LeaderId
	reply.Ack = true
	log.Printf("[Node %d] Receive NotifyLeader %d", rpcNode.node.id, newLeader)

	rpcNode.node.leaderId = newLeader
	rpcNode.node.isElectionRunning = false

	log.Printf("[Node %d] Acknowledge that leader is now %d", rpcNode.node.id, rpcNode.node.leaderId)
	return nil
}

// Heartbeat check
type HeartbeatReq struct {
	SenderID int
}
type HeartbeatRes struct {
	Alive bool
}

// Heartbeat: backups call coordinator to confirm it's alive
func (rpcNode *LeaderRPC) Heartbeat(req HeartbeatReq, res *HeartbeatRes) error {
	n := rpcNode.node
	n.electionMutex.Lock()
	defer n.electionMutex.Unlock()
	if n.id == n.leaderId {
		res.Alive = true
	} else {
		res.Alive = false
	}
	return nil
}

func (node *Node) becomeLeader() {
	node.electionMutex.Lock()
	defer node.electionMutex.Unlock()
	log.Printf("[Node %d] I am now the leader.\n", node.id)
	// Reassign and start Leader server only if it not is a leader before
	if node.leaderId != node.id {
		node.leaderId = node.id
		for _, nextId := range node.peerIds {
			log.Printf("[Node %d] Notify to node %d that leader is %d", node.id, nextId, node.id)
			var stepDownRes StepDownRes
			// Broadcast to other node to release port 8000
			err := node.callToLeader(
				nextId,
				"LeaderRPC.StepDown",
				StepDownReq{node.id},
				&stepDownRes,
			)
			if err != nil {
				log.Println(err)
			}
		}
		node.startLeaderServer()
	}

	// Notify to other nodes
	for _, nextId := range node.peerIds {
		log.Printf("[Node %d] Notify to node %d that leader is %d", node.id, nextId, node.id)
		var notifyLeaderRes NotifyLeaderRes
		// Don't care about ack
		err := node.callToFollower(
			nextId,
			"InternalRPC.NotifyLeader",
			NotifyLeaderReq{node.id},
			&notifyLeaderRes,
		)
		if err != nil {
			log.Println(err)
		}
	}
}

func (node *Node) startInternalServer(sharedDataBase *Database) {
	node.internalServer = rpc.NewServer()

	node.internalServer.Register(&InternalRPC{
		node: node,
	})

	port := 8000 + node.id
	addr := fmt.Sprintf(":%d", port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("[Node %d] Cannot listen on %s: %v", node.id, addr, err)
	}
	node.internalListener = l
	log.Printf("[Node %d] Internal server listening on %s", node.id, addr)
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				log.Printf("[Node %d] Internal listener closed: %v", node.id, err)
				return
			}
			go node.internalServer.ServeConn(conn)
		}
	}()
}

func (node *Node) startLeaderServer() {
	node.leaderServer = rpc.NewServer()
	node.leaderServer.Register(&LeaderRPC{
		node: node,
	})

	l, err := net.Listen("tcp", ":8000")
	if err != nil {
		log.Fatalf("[Node %d] Cannot listen on %s: %v", node.id, "8000", err)
	}
	node.leaderListener = l
	log.Printf("[Node %d] Leader server listening on %s", node.id, "8000")
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				log.Printf("[Node %d] Leader listener closed: %v", node.id, err)
				return
			}
			go node.leaderServer.ServeConn(conn)
		}
	}()
}

func (node *Node) startHeartbeatRoutine() {
	log.Printf("[Node %d] Start Heartbeat Rountine", node.id)
	go func() {
		for {
			time.Sleep(3 * time.Second)

			node.electionMutex.Lock()
			leader := node.leaderId
			node.electionMutex.Unlock()
			if leader == -1 {
				node.startElection()
				continue
			}
			if leader == node.id {
				continue
			}
			var heartbeatRes HeartbeatRes

			err := node.callToLeader(
				leader,
				"LeaderRPC.Heartbeat",
				HeartbeatReq{node.id},
				&heartbeatRes,
			)
			if err != nil || !heartbeatRes.Alive {
				log.Printf("[Node %d] Could not heartbeat to leader => Start electing ", node.id)
				node.startElection()
			}
		}
	}()
}

/*
User facing methods
*/

type SetKeyArgs struct {
	BucketName string
	Key        int
	Value      string
}

type GetKeyArgs struct {
	BucketName string
	Key        int
}

type DeleteKeyArgs struct {
	BucketName string
	Key        int
}

type Response struct {
	Data    string
	Message string
}

type EmptyRequest struct{}

func (server *LeaderRPC) SetKey(args *SetKeyArgs, reply *Response) error {
	database := server.node.database
	database.mutex.Lock()
	defer database.mutex.Unlock()
	database.db.Set(args.BucketName, args.Key, []byte(args.Value))
	reply.Message = "OK"
	server.doReplicate(ReplicateDataReq{
		BucketName: args.BucketName,
		Key:        args.Key,
		Value:      args.Value,
		Action:     "SET",
	})
	return nil
}

func (server *LeaderRPC) GetKey(args *GetKeyArgs, reply *Response) error {
	database := server.node.database

	data, isExist := database.db.Get(args.BucketName, args.Key)
	if !isExist {
		reply.Message = "Not found"
	} else {
		reply.Data = string(data)
	}

	return nil
}

func (server *LeaderRPC) DeleteKey(args *DeleteKeyArgs, reply *Response) error {
	database := server.node.database
	database.mutex.Lock()
	defer database.mutex.Unlock()
	ok, err := database.db.Del(args.BucketName, args.Key)
	if !ok {
		reply.Message = "Faild to delete key" + string(err.Error())
	} else {
		reply.Message = "OK"
	}
	server.doReplicate(ReplicateDataReq{
		BucketName: args.BucketName,
		Key:        args.Key,
		Value:      "",
		Action:     "DELETE",
	})
	return nil
}

func (server *LeaderRPC) GetStoreInfo(args EmptyRequest, reply *Response) error {
	database := server.node.database
	reply.Data = database.db.Info()
	return nil
}

/*
--------------
REPLICATE DATA
--------------
*/

type ReplicateDataReq struct {
	Action     string // SET|DELETE
	BucketName string
	Key        int
	Value      string
}

type ReplicateDataRes struct {
	IsSuccess bool
}

func (nodeRPC *InternalRPC) ReplicateAction(args ReplicateDataReq, reply *ReplicateDataRes) error {
	log.Printf("[Node %d] Receive replication request with action %s and key %s", nodeRPC.node.id, args.Action, args.BucketName)
	database := nodeRPC.node.database
	database.mutex.Lock()
	defer database.mutex.Unlock()
	if args.Action == "DELETE" {
		_, err := database.db.Del(args.BucketName, args.Key)
		if err != nil {
			return err
		}
	} else if args.Action == "SET" {
		err := database.db.Set(args.BucketName, args.Key, []byte(args.Value))
		if err != nil {
			return err
		}
	}
	return nil
}

func (leaderRPC *LeaderRPC) doReplicate(args ReplicateDataReq) {
	peerIds := leaderRPC.node.peerIds
	for _, peerId := range peerIds {
		var replicateRes ReplicateDataRes
		// Don't care about ack
		err := leaderRPC.node.callToFollower(
			peerId,
			"InternalRPC.ReplicateAction",
			args,
			&replicateRes,
		)
		if err != nil {
			log.Println(err)
		}
	}
}
