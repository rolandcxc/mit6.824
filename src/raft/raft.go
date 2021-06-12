package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sort"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const None = -1

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	timer *Timer

	state       StateType
	leader      int
	currentTerm int
	votedFor    int
	votes       map[int]bool

	tick tickFunc
	step stepFunc

	// number of ticks since it reached last electionTimeout when it is leader
	// or candidate.
	// number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int

	heartbeatTimeout int
	electionTimeout  int
	// randomizedElectionTimeout is a random number between
	// [electiontimeout, 2 * electiontimeout - 1]. It gets reset
	// when raft changes its state to follower or candidate.
	randomizedElectionTimeout int

	lastHBTime time.Time
	raftLog    *raftLog

	rpcProxy *proxy

	applyCh chan ApplyMsg
}

type StateType string

const (
	StateLeader    StateType = "Leader"
	StateCandidate StateType = "Candidate"
	StateFollower  StateType = "Follower"
)

type raftLog struct {
	entries   []*Entry
	lastEntry *Entry
	offset    int // index-offset为该条日志在entries中的位置

	// soft state for all roles
	commitIndex int
	lastApplied int

	// soft state for leader
	nextIndex  []int
	matchIndex []int
}

func (l *raftLog) lastIndex() int {
	return l.lastEntry.Index
}

func (l *raftLog) peerNext(i int) int {
	return l.nextIndex[i]
}

func (l *raftLog) getEntries(start int) []*Entry {
	if !l.validIndex(start) {
		// TODO Panic
		return nil
	}
	return l.entries[start-l.offset:]
}

func (l *raftLog) validIndex(index int) bool {
	if index-l.offset > len(l.entries) {
		return false
	}
	if index-l.offset < 0 {
		return false
	}
	return true
}

// 0 表示不存在该日志
func (l *raftLog) term(index int) int {
	// TODO 校验index
	if !l.validIndex(index) {
		return 0
	}
	return l.entries[index-l.offset].Term
}

func (l *raftLog) lastTerm() int {
	return l.lastEntry.Term
}

func (l *raftLog) last() *Entry {
	return l.lastEntry
}

func (l *raftLog) isUpToDate(index, term int) bool {
	return term > l.lastTerm() || (term == l.lastTerm() && index >= l.lastIndex())
}

func (l *raftLog) appendEntries(entries []*Entry, preIndex int) {
	l.entries = l.entries[:preIndex-l.offset+1]
	l.entries = append(l.entries, entries...)
	l.lastEntry = entries[len(entries)-1]
}

func (l *raftLog) appendSingleEntry(entry *Entry) {
	l.entries = append(l.entries, entry)
	l.lastEntry = entry
}

func (rf *Raft) getCurrentTerm() int {
	return rf.currentTerm
}

func (rf *Raft) reset(term int) {
	if term != rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = 0
	}
	rf.leader = None
}

func (rf *Raft) setCurrentTerm(v int) {
	rf.currentTerm = v
}

type Entry struct {
	Term  int
	Index int
	Data  interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	return rf.currentTerm, rf.leader == rf.me
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//defer func() {
	//	DPrintf("[RequestVote]%d, reply: %+v, term: %d, voted: %d", rf.me, *reply, rf.currentTerm, rf.votedFor)
	//}()

	if args.Term < rf.getCurrentTerm() {
		reply.VoteGranted = false
		reply.Term = rf.getCurrentTerm()
		return
	}

	if args.Term > rf.getCurrentTerm() {
		rf.becomeFollower(args.Term, args.CandidateId)
	}

	if (rf.votedFor == 0 || args.CandidateId == rf.votedFor) &&
		rf.raftLog.isUpToDate(args.LastLogIndex, args.LastLogTerm) {
		rf.becomeFollower(args.Term, args.CandidateId)

		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = rf.getCurrentTerm()
		DPrintf("[%d] vote to %d", rf.me, args.CandidateId)
		return
	}

	reply.VoteGranted = false
	reply.Term = rf.getCurrentTerm()
	return

	// Your code here (2A, 2B).
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendEvent(eventType string, server int, args interface{}) {
	rf.rpcProxy.req <- &message{
		messageType: eventType,
		to:          server,
		args:        args,
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term       int
	Success    bool
	RejectHint int
	RejectTerm int
}

func (rf *Raft) AppendEntries(arg *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	defer func() {
		DPrintf("[AppendEntries]%d, reply: %+v, term: %d, lastIndex: %d, commit: %d", rf.me, *reply, rf.currentTerm, rf.raftLog.lastIndex(), rf.raftLog.commitIndex)
	}()

	if arg.Term < rf.getCurrentTerm() {
		reply.Term = rf.getCurrentTerm()
		reply.Success = false
		return
	}

	if arg.Term == rf.getCurrentTerm() && rf.leader != None && rf.leader != arg.LeaderID {
		DPrintf("%d receive Entries from %d, but leader is %d, term is %d", rf.me, arg.LeaderID, rf.leader, rf.currentTerm)
	}

	rf.becomeFollower(arg.Term, arg.LeaderID)

	term := rf.raftLog.term(arg.PrevLogIndex)
	if term == 0 || term != arg.PrevLogTerm {
		reply.Term = rf.getCurrentTerm()
		reply.Success = false
		return
	}

	if len(arg.Entries) > 0 {
		rf.raftLog.appendEntries(arg.Entries, arg.PrevLogIndex)
	}

	if arg.LeaderCommit > rf.raftLog.commitIndex {
		rf.raftLog.commitIndex = min(arg.LeaderCommit, rf.raftLog.lastIndex())
		rf.apply()
	}

	reply.Term = rf.getCurrentTerm()
	reply.Success = true
	return
}

func (l *raftLog) findConflictByTerm(index int, term int) int {
	if li := l.lastIndex(); index > li {
		return index
	}
	for {
		logTerm := l.term(index)
		if logTerm <= term {
			break
		}
		index--
	}
	return index
}


func (rf *Raft) apply() {
	DPrintf("start apply!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
	rl := rf.raftLog
	if rl.lastApplied == rl.commitIndex {
		return
	}

	entries := rl.entries[rl.lastApplied-rl.offset+1 : rl.commitIndex-rl.offset+1]
	rl.lastApplied = rl.commitIndex

	for _, e := range entries {
		m := ApplyMsg{
			CommandValid: true,
			Command:      e.Data,
			CommandIndex: e.Index - 1,
		}

		rf.applyCh <- m
	}

	DPrintf("%d lastApplied: %d", rf.me, rl.lastApplied)
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) campaign() {
	rf.becomeCandidate()

	arg := &RequestVoteArgs{
		Term:         rf.getCurrentTerm(),
		CandidateId:  rf.me,
		LastLogIndex: rf.raftLog.lastIndex(),
		LastLogTerm:  rf.raftLog.lastTerm(),
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.sendEvent(MsgVote, i, arg)
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.leader != rf.me {
		return -1, -1, false
	}

	//data, _ := json.Marshal(command)

	newEntry := &Entry{
		Term:  rf.currentTerm,
		Index: rf.raftLog.lastIndex() + 1,
		Data:  command,
	}
	rf.raftLog.appendSingleEntry(newEntry)
	rf.raftLog.matchIndex[rf.me] = rf.raftLog.lastIndex()
	rf.sendEntryToAll()

	// Your code here (2B).

	return newEntry.Index - 1, newEntry.Term, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) coreLoop() {
	for rf.killed() == false {
		select {
		case <-rf.timer.C:
			rf.handleTick()
		case msg := <-rf.rpcProxy.resp:
			rf.handleMsg(msg)
		}
	}
}

func (rf *Raft) handleTick() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.tick()
}

func (rf *Raft) handleMsg(msg *message) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if msg.term < rf.getCurrentTerm() {
		return
	}

	if msg.term > rf.getCurrentTerm() {
		rf.becomeFollower(msg.term, msg.from)
		return
	}

	switch msg.messageType {
	case MsgVoteResp:
		if rf.state != StateCandidate {
			return
		}

		if msg.accept {
			rf.votes[msg.from] = true
		}
		if len(rf.votes) >= 1+len(rf.peers)/2 {
			rf.becomeLeader()
			rf.sendEntryToAll()
		}
	case MsgAppendResp:
		if rf.state != StateLeader {
			return
		}
		args := msg.args.(*AppendEntriesArgs)

		l := rf.raftLog

		if msg.accept {
			l.matchIndex[msg.from] = args.PrevLogIndex + len(args.Entries)
			l.nextIndex[msg.from] = args.PrevLogIndex + len(args.Entries) + 1

			matches := make([]int, 0, len(l.matchIndex))
			matches = append(matches, l.matchIndex...)
			sort.Slice(matches, func(i, j int) bool {
				return matches[i] > matches[j]
			})

			for n := matches[len(l.matchIndex)/2]; n > l.commitIndex; n-- {
				if l.term(n) == rf.currentTerm {
					l.commitIndex = n
					rf.apply()
					break
				}
			}
			return
		}

		l.nextIndex[msg.from]--
		rf.sendEntryTo(msg.from)
		return
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(time.Millisecond * 20)
		rf.timer.tick()
	}
}

type tickFunc func()

func (rf *Raft) tickElection() {
	rf.electionElapsed++

	if rf.electionElapsed >= rf.randomizedElectionTimeout {
		rf.electionElapsed = 0
		rf.campaign()
	}
}

func (rf *Raft) tickHeartBeat() {
	rf.heartbeatElapsed++

	if rf.heartbeatElapsed >= rf.heartbeatTimeout {
		rf.heartbeatElapsed = 0
		rf.sendEntryToAll()
	}
}

func (rf *Raft) resetRandomizedElectionTimeout() {
	rf.randomizedElectionTimeout = rf.electionTimeout + rand.Intn(rf.electionTimeout)
}

func (rf *Raft) sendEntryToAll() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		rf.sendEntryTo(i)
	}
}

func (rf *Raft) sendEntryTo(peer int) {
	rl := rf.raftLog

	if peer == rf.me {
		return
	}

	args := &AppendEntriesArgs{
		Term:         rf.getCurrentTerm(),
		LeaderID:     rf.me,
		PrevLogIndex: rl.lastIndex(),
		PrevLogTerm:  rl.lastTerm(),
		Entries:      nil,
		LeaderCommit: rf.raftLog.commitIndex,
	}

	next := rl.peerNext(peer)
	if rl.lastIndex() >= next {
		args.Entries = rl.getEntries(next)
		args.PrevLogIndex = next - 1
		args.PrevLogTerm = rl.term(next - 1)

		if args.PrevLogTerm == 0 {
			panic("args.PrevLogTerm == 0")
		}
	}

	DPrintf("%d send entry to %d, term: %d, lastIndex: %d, args: %+v", rf.me, peer, rf.getCurrentTerm(), rf.raftLog.lastIndex(), *args)
	rf.sendEvent(MsgAppend, peer, args)
}

type stepFunc func()

func (rf *Raft) stepFollower() {

}

func (rf *Raft) stepLeader() {

}

func (rf *Raft) stepCandidate() {

}

func (rf *Raft) becomeCandidate() {
	DPrintf("peer[%d] becomeCandidate, term: %d, time: %v", rf.me, rf.getCurrentTerm()+1, time.Now())
	rf.state = StateCandidate
	rf.reset(rf.currentTerm + 1)
	rf.votedFor = rf.me
	rf.resetRandomizedElectionTimeout()
	rf.step = rf.stepCandidate
	rf.tick = rf.tickElection

	rf.votes = make(map[int]bool)
	rf.votes[rf.me] = true

	rf.heartbeatElapsed = 0
	rf.electionElapsed = 0
}

func (rf *Raft) becomeLeader() {
	DPrintf("peer[%d] becomeLeader, ############%d, term: %d", rf.me, rf.me, rf.getCurrentTerm())

	rf.state = StateLeader
	rf.leader = rf.me
	rf.step = rf.stepLeader
	rf.tick = rf.tickHeartBeat

	rf.heartbeatElapsed = 0
	rf.electionElapsed = 0

	rf.raftLog.matchIndex = make([]int, len(rf.peers))
	rf.raftLog.nextIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.raftLog.nextIndex[i] = rf.raftLog.lastIndex() + 1
	}
	DPrintf("leader %d's nextIndex: %v", rf.me, rf.raftLog.nextIndex)
	rf.raftLog.matchIndex[rf.me] = rf.raftLog.lastIndex()
}

func (rf *Raft) becomeFollower(term int, leader int) {
	if leader != rf.leader {
		DPrintf("peer[%d] becomeFollower, leader: %d, term: %d", rf.me, leader, term)
	}

	rf.state = StateFollower
	rf.reset(term)
	rf.leader = leader
	rf.resetRandomizedElectionTimeout()
	rf.step = rf.stepFollower
	rf.tick = rf.tickElection

	rf.heartbeatElapsed = 0
	rf.electionElapsed = 0
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rand.Seed(int64(me) + time.Now().Unix())

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.timer = NewTimer()
	rf.heartbeatTimeout = 5
	rf.electionTimeout = 50
	rf.resetRandomizedElectionTimeout()
	rf.applyCh = applyCh
	//DPrintf("peer[%d] resetRandomizedElectionTimeout: %d", me, rf.randomizedElectionTimeout)

	rf.becomeFollower(1, None)
	initEntry := &Entry{
		Term:  1,
		Index: 1,
		Data:  nil,
	}

	rf.raftLog = &raftLog{
		entries:     []*Entry{initEntry},
		lastEntry:   initEntry,
		offset:      1,
		commitIndex: 1,
		lastApplied: 1,
	}

	rf.rpcProxy = newProxy(rf, 20)
	rf.rpcProxy.start()
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.coreLoop()

	return rf
}

type Timer struct {
	C chan struct{}
}

func NewTimer() *Timer {
	return &Timer{C: make(chan struct{}, 100)}
}

func (t *Timer) tick() {
	select {
	case t.C <- struct{}{}:
	default:
		DPrintf("timer block")
	}
}
