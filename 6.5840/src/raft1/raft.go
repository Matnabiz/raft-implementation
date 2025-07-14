package raft

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// Define server states
const (
	Follower = iota
	Candidate
	Leader
)

// LogEntry for commands
type LogEntry struct {
	Command interface{}
	Term    int
}

// Raft struct
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *tester.Persister
	me        int
	dead      int32

	// Persistent state
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state
	commitIndex int
	lastApplied int

	// Leader state
	nextIndex  []int
	matchIndex []int

	// Role and timing
	state         int
	electionReset time.Time

	// Apply channel
	applyCh chan raftapi.ApplyMsg

	lastSnapshotIndex int // the index up through which we've snapshotted
	lastSnapshotTerm  int
}

// RequestVoteArgs as per paper
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply as per paper
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// AppendEntriesArgs for log replication + heartbeats
type AppendEntriesArgs struct {
	ConflictIndex int
	ConflictTerm  int
	Term          int
	LeaderId      int
	PrevLogIndex  int
	PrevLogTerm   int
	Entries       []LogEntry
	LeaderCommit  int
}

// AppendEntriesReply
type AppendEntriesReply struct {
	ConflictIndex int
	ConflictTerm  int
	Term          int
	Success       bool
	NextTry       int
}

// InstallSnapshot RPC args & reply
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int    // the snapshot replaces all entries up through this index
	LastIncludedTerm  int    // term of the last included entry
	Data              []byte // the snapshot blob
}

type InstallSnapshotReply struct {
	Term int
}

// Snapshot is called by the service to tell Raft:
// "I have created a snapshot that includes all
// Raft log entries up to and including index.
// Please discard any prior log entries and persist
// the snapshot." The snapshot []byte is the raw serialized
// snapshot produced by the service.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastSnapshotIndex {
		return
	}
	// 1) compute term of entry being snapshotted
	logIndex := index - rf.lastSnapshotIndex
	lastTerm := rf.log[logIndex].Term

	// 2) truncate log with dummy at [0]
	newLog := make([]LogEntry, 1, len(rf.log)-logIndex+1)
	newLog[0] = LogEntry{Term: lastTerm}
	newLog = append(newLog, rf.log[logIndex+1:]...)
	rf.log = newLog

	// 3) advance snapshot metadata
	rf.lastSnapshotIndex = index
	rf.lastSnapshotTerm = lastTerm

	// 4) advance commitIndex & lastApplied so we don't re‑apply
	if rf.commitIndex < index {
		rf.commitIndex = index
	}
	if rf.lastApplied < index {
		rf.lastApplied = index
	}

	// 5) fix nextIndex/matchIndex for all peers
	for i := range rf.peers {
		if rf.nextIndex[i] < index+1 {
			rf.nextIndex[i] = index + 1
		}
		if rf.matchIndex[i] < index {
			rf.matchIndex[i] = index
		}
	}

	// 6) persist Raft state + snapshot atomically
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastSnapshotIndex)
	e.Encode(rf.lastSnapshotTerm)
	e.Encode(rf.log)
	state := w.Bytes()
	rf.persister.SaveStateAndSnapshot(state, snapshot)

	// 7) if leader, immediately replicate so followers install snapshot
	if rf.state == Leader {
		for srv := range rf.peers {
			if srv == rf.me {
				continue
			}
			go rf.replicateLogToPeer(srv)
		}
	}
}

// GetState returns currentTerm and isLeader
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// Start appends a new command to leader's log
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}
	index := len(rf.log)

	rf.log = append(rf.log, LogEntry{Command: command, Term: rf.currentTerm})
	rf.persist()
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	// trigger replication
	go rf.replicateLog()
	return index, rf.currentTerm, true
}

// PersistBytes stub
func (rf *Raft) PersistBytes() int {
	return 0
}

// Kill signals server shutdown
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

// killed checks if server is shutdown
func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

// sendRequestVote RPC
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

// sendAppendEntries RPC
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

// RequestVote handler
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	}
	// new term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.persist()
		}
		rf.votedFor = -1
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.persist()
		}
		rf.state = Follower
	}
	// log up-to-date check
	lastIndex := len(rf.log) - 1
	lastTerm := rf.log[lastIndex].Term
	if args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex) {
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.electionReset = time.Now()
		}
	}
}

// AppendEntries handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1) Reply current term
	reply.Term = rf.currentTerm

	// 2) If term is stale, reject
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	// 3) If we see a higher term, step down and persist it
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	rf.state = Follower
	rf.electionReset = time.Now()

	// 4) Check for log consistency at PrevLogIndex/PrevLogTerm
	if args.PrevLogIndex >= len(rf.log) ||
		(args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {

		// indicate failure and provide conflict info
		reply.Success = false
		if args.PrevLogIndex >= len(rf.log) {
			reply.ConflictIndex = len(rf.log)
			reply.ConflictTerm = -1
		} else {
			conflictTerm := rf.log[args.PrevLogIndex].Term
			// find first index of that term
			idx := args.PrevLogIndex
			for idx > 0 && rf.log[idx-1].Term == conflictTerm {
				idx--
			}
			reply.ConflictTerm = conflictTerm
			reply.ConflictIndex = idx
		}
		return
	}

	// 5) Append new entries, truncating any conflicts
	newIndex := args.PrevLogIndex + 1
	i := 0
	for ; i < len(args.Entries); i++ {
		if newIndex+i >= len(rf.log) {
			break
		}
		if rf.log[newIndex+i].Term != args.Entries[i].Term {
			// conflict found → truncate
			rf.log = rf.log[:newIndex+i]
			break
		}
	}
	// append any remaining entries
	if i < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[i:]...)
	}
	// persist log change
	rf.persist()

	// 6) Update reply
	reply.Success = true

	// 7) Update commitIndex and apply if needed
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		go rf.applyEntries()
	}
}

// replicateLog sends AppendEntries to all followers
// replicateLog fires off replication to all followers.
func (rf *Raft) replicateLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// only the leader should call this
	if rf.state != Leader {
		return
	}
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go rf.replicateLogToPeer(server)
	}
}

// in raft.go, alongside sendAppendEntries and sendRequestVote:

// sendInstallSnapshot RPC sender.
// Calls the InstallSnapshot handler on the target server.
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}
	// 1) Step down on new term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	rf.state = Follower
	rf.electionReset = time.Now()

	// 2) If this snapshot is newer, install it
	if args.LastIncludedIndex > rf.lastSnapshotIndex {
		oldSnapIndex := rf.lastSnapshotIndex

		// update snapshot metadata
		rf.lastSnapshotIndex = args.LastIncludedIndex
		rf.lastSnapshotTerm = args.LastIncludedTerm

		// discard log prefix up through LastIncludedIndex
		newLog := make([]LogEntry, 0)
		for i, entry := range rf.log {
			globalIdx := oldSnapIndex + i
			if globalIdx > args.LastIncludedIndex {
				newLog = append(newLog, entry)
			}
		}
		// prepend dummy for the snapshot position
		rf.log = append([]LogEntry{{Term: args.LastIncludedTerm}}, newLog...)

		// **CRITICAL**: advance both commitIndex and lastApplied
		rf.commitIndex = args.LastIncludedIndex
		rf.lastApplied = args.LastIncludedIndex

		// persist state+snapshot
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(rf.currentTerm)
		e.Encode(rf.votedFor)
		e.Encode(rf.lastSnapshotIndex)
		e.Encode(rf.lastSnapshotTerm)
		e.Encode(rf.log)
		state := w.Bytes()
		rf.persister.SaveStateAndSnapshot(state, args.Data)

		// 3) deliver the snapshot to the service
		rf.mu.Unlock()
		rf.applyCh <- raftapi.ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
		rf.mu.Lock()
	}
}

// replicateLogToPeer handles AppendEntries RPCs (and retries) for one follower.
// replicateLogToPeer handles both AppendEntries and InstallSnapshot as needed.
func (rf *Raft) replicateLogToPeer(server int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	term := rf.currentTerm
	prevIndex := rf.nextIndex[server] - 1
	commitIndex := rf.commitIndex

	// If the follower is so far behind it needs a snapshot:
	if prevIndex < rf.lastSnapshotIndex {
		// build and send InstallSnapshot
		args := &InstallSnapshotArgs{
			Term:              term,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.lastSnapshotIndex,
			LastIncludedTerm:  rf.lastSnapshotTerm,
			Data:              rf.persister.ReadSnapshot(),
		}
		rf.mu.Unlock()

		reply := &InstallSnapshotReply{}
		if rf.sendInstallSnapshot(server, args, reply) {
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.state = Follower
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.persist()
			} else {
				// follower has installed snapshot, catch up nextIndex/matchIndex
				rf.nextIndex[server] = rf.lastSnapshotIndex + 1
				rf.matchIndex[server] = rf.lastSnapshotIndex
			}
			rf.mu.Unlock()
		}
		return
	}

	// Otherwise send AppendEntries as before
	// bounds check prevIndex
	if prevIndex >= len(rf.log) {
		prevIndex = len(rf.log) - 1
	}
	if prevIndex < -1 {
		prevIndex = -1
	}

	var prevTerm int
	if prevIndex >= 0 {
		prevTerm = rf.log[prevIndex].Term
	} else {
		prevTerm = -1
	}

	entries := make([]LogEntry, len(rf.log[prevIndex+1:]))
	copy(entries, rf.log[prevIndex+1:])
	rf.mu.Unlock()

	// build and send AppendEntries RPC
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: commitIndex,
	}
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// term check
	if reply.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persist()
		return
	}

	if reply.Success {
		rf.matchIndex[server] = prevIndex + len(entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		// advance commitIndex if possible...
		for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
			if rf.log[N].Term != rf.currentTerm {
				continue
			}
			count := 1
			for peer := range rf.peers {
				if peer != rf.me && rf.matchIndex[peer] >= N {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				rf.commitIndex = N
				go rf.applyEntries()
				break
			}
		}
	} else {
		// back off nextIndex using conflict hints
		if reply.ConflictTerm != -1 {
			last := -1
			for i := len(rf.log) - 1; i >= 0; i-- {
				if rf.log[i].Term == reply.ConflictTerm {
					last = i
					break
				}
			}
			if last >= 0 {
				rf.nextIndex[server] = last + 1
			} else {
				rf.nextIndex[server] = reply.ConflictIndex
			}
		} else {
			rf.nextIndex[server] = reply.ConflictIndex
		}
		if rf.nextIndex[server] < 1 {
			rf.nextIndex[server] = 1
		}
		// retry
		go rf.replicateLogToPeer(server)
	}
}

// applyEntries sends committed logs to applyCh
func (rf *Raft) applyEntries() {
	rf.mu.Lock()
	for rf.lastApplied+1 <= rf.commitIndex && rf.lastApplied+1 < len(rf.log) {
		rf.lastApplied++
		entry := rf.log[rf.lastApplied]
		rf.mu.Unlock()

		rf.applyCh <- raftapi.ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: rf.lastApplied,
		}

		rf.mu.Lock()
	}

	rf.mu.Unlock()
}

// ticker runs election timeout checks
func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		last := rf.electionReset
		rf.mu.Unlock()

		timeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
		if state != Leader && time.Since(last) >= timeout {
			go rf.startElection()
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// startElection begins a new election
func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.electionReset = time.Now()
	term := rf.currentTerm
	lastIndex := len(rf.log) - 1
	lastTerm := rf.log[lastIndex].Term
	rf.mu.Unlock()

	var votes int32 = 1
	var wg sync.WaitGroup

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		wg.Add(1)
		go func(server int) {
			defer wg.Done()
			args := &RequestVoteArgs{term, rf.me, lastIndex, lastTerm}
			reply := &RequestVoteReply{}

			if rf.sendRequestVote(server, args, reply) {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.state = Follower
					rf.currentTerm = reply.Term
					rf.votedFor = -1
				}
				if reply.VoteGranted && rf.state == Candidate && rf.currentTerm == term {
					atomic.AddInt32(&votes, 1)
				}
				rf.mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Candidate && votes > int32(len(rf.peers)/2) {
		rf.state = Leader
		// initialize leader state
		for i := range rf.peers {
			rf.nextIndex[i] = len(rf.log)
			rf.matchIndex[i] = 0
		}
		// start heartbeats
		go rf.sendHeartbeats()
	}
}

// sendHeartbeats sends empty AppendEntries periodically
func (rf *Raft) sendHeartbeats() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		rf.mu.Unlock()

		// send immediately on loop entry
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go rf.replicateLogToPeer(i)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func Make(peers []*labrpc.ClientEnd, me int, persister *tester.Persister, applyCh chan raftapi.ApplyMsg) *Raft {
	rf := &Raft{
		peers:         peers,
		persister:     persister,
		me:            me,
		state:         Follower,
		currentTerm:   0,
		votedFor:      -1,
		log:           []LogEntry{{}}, // index 0 is a placeholder
		electionReset: time.Now(),
		applyCh:       applyCh,
	}

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := range peers {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}
	rf.readPersist(rf.persister.ReadRaftState())
	// If there’s a snapshot on disk, send it up to the service now
	snapshot := rf.persister.ReadSnapshot()
	if snapshot != nil && len(snapshot) > 0 {
		rf.applyCh <- raftapi.ApplyMsg{
			SnapshotValid: true,
			Snapshot:      snapshot,
			SnapshotTerm:  rf.lastSnapshotTerm,
			SnapshotIndex: rf.lastSnapshotIndex,
		}
	}

	// start the election ticker
	go rf.ticker()

	return rf
}

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// encode the three persistent fields:
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastSnapshotIndex)
	e.Encode(rf.lastSnapshotTerm)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term, votedFor, lastSnapIndex, lastSnapTerm int
	var log []LogEntry
	if d.Decode(&term) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&lastSnapIndex) != nil ||
		d.Decode(&lastSnapTerm) != nil ||
		d.Decode(&log) != nil {
		// ignore decode errors
	} else {
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.lastSnapshotIndex = lastSnapIndex
		rf.lastSnapshotTerm = lastSnapTerm
		rf.log = log
	}
}
