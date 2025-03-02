package main

import (
	"log"
	"time"
)

type PingRequest struct {
	SenderID int
}

type PingReply struct {
	Alive bool
}

type ElectionRequest struct {
	SenderID int
}
type ElectionReply struct {
	Ack bool
}

type StepDownRequest struct {
	SenderID int
}

type StepDownReply struct {
	Ok bool
}

type NotifyLeaderRequest struct {
	LeaderId int
}

type NotifyLeaderReply struct {
	Ack bool
}

func (n *Node) startElection() {
	n.electionMutex.Lock()
	if n.isElectionGoging {
		n.electionMutex.Unlock()
		return
	}
	n.isElectionGoging = true
	n.electionMutex.Unlock()

	higherIds := []int{}
	for _, peerId := range n.peerIds {
		if peerId > n.id {
			higherIds = append(higherIds, peerId)
		}
	}
	var isFoundHigherActiveNode bool = false
	for _, nextId := range higherIds {
		log.Printf("[Node %d] Call Election to n %d ", n.id, nextId)
		var electionReply ElectionReply
		err := n.callToPeer(
			nextId,
			"InternalRPC.Election",
			ElectionRequest{n.id},
			&electionReply,
		)
		if err != nil {
			log.Println("Has error while trying to get key", err)
		} else if electionReply.Ack {
			// Have another higher n to delegage. Abort
			isFoundHigherActiveNode = true
			break
		}
	}

	if !isFoundHigherActiveNode {
		n.isElectionGoging = false
		// TODO: promote to leader and notified all other n
		n.becomeLeader()
	}
}

func (n *Node) startPingRoutine() {
	log.Printf("[Node %d] Start Ping Rountine", n.id)
	go func() {
		for {
			time.Sleep(3 * time.Second)

			n.electionMutex.Lock()
			leader := n.leaderId
			n.electionMutex.Unlock()
			if leader == -1 {
				n.startElection()
				continue
			} else if leader == n.id {
				continue
			}
			var PingReply PingReply

			err := n.callToLeader(
				leader,
				"LeaderRPC.Ping",
				PingRequest{n.id},
				&PingReply,
			)
			if err != nil || !PingReply.Alive {
				log.Printf("[Node %d] Could not ping to leader => Start electing ", n.id)
				n.startElection()
			}
		}
	}()
}

func (n *Node) becomeLeader() {
	n.electionMutex.Lock()
	defer n.electionMutex.Unlock()
	log.Printf("[Node %d] is now the leader.\n", n.id)
	// Reasign and start Leader server only if it is not a leader before
	if n.leaderId != n.id {
		n.leaderId = n.id
		for _, nextId := range n.peerIds {
			log.Printf("[Node %d] Notify to [Node %d] that leader is now [Node %d]", n.id, nextId, n.id)
			var stepDownReply StepDownReply
			// Broadcast to other n to release port 8000
			err := n.callToLeader(
				nextId,
				"LeaderRPC.StepDown",
				StepDownRequest{n.id},
				&stepDownReply,
			)
			if err != nil {
				log.Println(err)
			}
		}
		n.startLeaderServer()
	}

	// Notify to other nodes
	for _, nextId := range n.peerIds {
		log.Printf("[Node %d] Notify to n %d that leader is %d", n.id, nextId, n.id)
		var notifyLeaderReply NotifyLeaderReply
		// Don't care about ack
		err := n.callToPeer(
			nextId,
			"InternalRPC.NotifyLeader",
			NotifyLeaderRequest{n.id},
			&notifyLeaderReply,
		)
		if err != nil {
			log.Println(err)
		}
	}
}

func (i *InternalRPC) NotifyLeader(args NotifyLeaderRequest, reply *NotifyLeaderReply) error {
	newLeader := args.LeaderId
	reply.Ack = true
	log.Printf("[Node %d] Receive NotifyLeader %d", i.node.id, newLeader)
	// If leaderId is smaller than current leaderId, no need to change

	i.node.leaderId = newLeader
	i.node.isElectionGoging = false

	log.Printf("[Node %d] Acknowledge that leader is now %d", i.node.id, i.node.leaderId)
	return nil
}

func (i *InternalRPC) Election(args ElectionRequest, reply *ElectionReply) error {
	// When you receive this. response with OK then continue to call Election to the next node
	reply.Ack = true

	// Check if this node is start election process or not. If not, start one
	if !i.node.isElectionGoging {
		i.node.startElection()
	}
	return nil
}
