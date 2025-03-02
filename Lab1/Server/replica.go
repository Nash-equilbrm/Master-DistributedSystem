package main

import "log"

type ReplicateDataRequest struct {
	Action     string
	BucketName string
	Key        int
	Value      []byte
}

type ReplicateDataReply struct {
	Success bool
}

func (i *InternalRPC) ReplicateAction(req ReplicateDataRequest, reply *ReplicateDataReply) error {
	log.Printf("[Node %d] Receive replication request with action %s and key %s", i.node.id, req.Action, req.BucketName)
	database := i.node.database
	database.mutex.Lock()
	defer database.mutex.Unlock()
	if req.Action == "DELETE" {
		_, err := database.db.Del(req.BucketName, req.Key)
		if err != nil {
			return err
		}
	} else if req.Action == "SET" {
		err := database.db.Set(req.BucketName, req.Key, []byte(req.Value))
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *LeaderRPC) doReplicate(req ReplicateDataRequest) {
	peerIds := l.node.peerIds
	for _, peerId := range peerIds {
		var replicateReply ReplicateDataReply
		err := l.node.callToPeer(
			peerId,
			"InternalRPC.ReplicateAction",
			req,
			&replicateReply,
		)
		if err != nil {
			log.Println(err)
		}
	}
}
