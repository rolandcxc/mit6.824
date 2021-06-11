package raft

import (
	"6.824/labrpc"
)

const (
	MsgVote       = "RequestVote"
	MsgAppend     = "AppendEntries"
	MsgVoteResp   = "RequestVoteResp"
	MsgAppendResp = "AppendEntriesResp"
)

type message struct {
	messageType string
	from        int
	to          int
	args        interface{}
	term        int
	accept      bool
}

type proxy struct {
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	workerNum int
	req       chan *message
	resp      chan *message
	stop      <-chan struct{}
}

func newProxy(rf *Raft, workerNum int) *proxy {
	return &proxy{
		stop:      make(chan struct{}, 1),
		peers:     rf.peers,
		workerNum: workerNum,
		req:       make(chan *message, workerNum*100),
		resp:      make(chan *message, workerNum*100*len(rf.peers)),
	}
}

func (p *proxy) start() {
	for i := 1; i <= p.workerNum; i++ {
		go func(id int) {
			for e := range p.req {
				if p.send(e) {
					p.resp <- e
				}
			}
		}(i)
	}
}

func (p *proxy) send(msg *message) bool {
	var ok bool
	switch msg.messageType {
	case MsgAppend:
		reply := &AppendEntriesReply{}
		ok = p.peers[msg.to].Call("Raft."+MsgAppend, msg.args, reply)
		msg.from = msg.to
		msg.messageType = MsgAppendResp
		msg.term = reply.Term
		msg.accept = reply.Success
	case MsgVote:
		reply := &RequestVoteReply{}
		ok = p.peers[msg.to].Call("Raft."+MsgVote, msg.args, reply)
		msg.from = msg.to
		msg.messageType = MsgVoteResp
		msg.term = reply.Term
		msg.accept = reply.VoteGranted
	}
	return ok
}
