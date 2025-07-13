package raft

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

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
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	// trigger replication
	go rf.replicateLog()
	return index, rf.currentTerm, true
}

// Snapshot stub
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// not implemented in 3B
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
		rf.votedFor = -1
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

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	rf.state = Follower
	rf.currentTerm = args.Term
	rf.votedFor = -1 // optional but safe
	rf.electionReset = time.Now()

	// Check if PrevLogIndex is out of bounds
	if args.PrevLogIndex >= len(rf.log) {
		reply.Success = false
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = -1
		return
	}

	// Check for conflicting term at PrevLogIndex
	if args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		conflictTerm := rf.log[args.PrevLogIndex].Term
		reply.ConflictTerm = conflictTerm

		// find the first index of that term
		index := args.PrevLogIndex
		for index > 0 && rf.log[index-1].Term == conflictTerm {
			index--
		}
		reply.ConflictIndex = index
		return
	}

	// Append any new entries (skip matching prefix)
	i := 0
	for i < len(args.Entries) {
		pos := args.PrevLogIndex + 1 + i
		if pos >= len(rf.log) {
			rf.log = rf.log[:pos] // ensure truncate
			break
		}
		if rf.log[pos].Term != args.Entries[i].Term {
			rf.log = rf.log[:pos]
			break
		}
		i++
	}
	rf.log = append(rf.log, args.Entries[i:]...)

	reply.Success = true

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

// replicateLogToPeer handles AppendEntries RPCs (and retries) for one follower.
func (rf *Raft) replicateLogToPeer(server int) {
	// Capture a snapshot of currentTerm so we don't mix terms across calls
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	term := rf.currentTerm
	prevIndex := rf.nextIndex[server] - 1
	commitIndex := rf.commitIndex

	// bounds check prevIndex
	if prevIndex >= len(rf.log) {
		prevIndex = len(rf.log) - 1
	}
	if prevIndex < -1 {
		prevIndex = -1
	}

	// capture prevTerm
	var prevTerm int
	if prevIndex >= 0 {
		prevTerm = rf.log[prevIndex].Term
	} else {
		prevTerm = -1
	}

	// copy out the entries to send
	entries := make([]LogEntry, len(rf.log[prevIndex+1:]))
	copy(entries, rf.log[prevIndex+1:])
	rf.mu.Unlock()

	// build and send the RPC
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
		// RPC failed (e.g., network partition)—try again on next heartbeat
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if leader discovered a newer term, step down
	if reply.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		return
	}

	if reply.Success {
		// update matchIndex & nextIndex
		rf.matchIndex[server] = prevIndex + len(entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1

		// advance commitIndex if possible (only entries in current term)
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
		// back off nextIndex based on conflict hint
		if reply.ConflictTerm != -1 {
			// find the last index of that term in our log
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
		// retry immediately (you could also wait a few ms)
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
		term := rf.currentTerm
		commitIndex := rf.commitIndex
		rf.mu.Unlock()

		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			go func(server int) {
				rf.mu.Lock()
				// Defensive: check that server is within bounds
				if server >= len(rf.nextIndex) {
					rf.mu.Unlock()
					return
				}
				prevIndex := rf.nextIndex[server] - 1

				var prevTerm int
				if prevIndex >= 0 && prevIndex < len(rf.log) {
					prevTerm = rf.log[prevIndex].Term
				} else {
					// If prevIndex is out of range, treat it as if there's no prior log
					prevTerm = -1 // or 0, depending on how you initialized logs
				}

				args := &AppendEntriesArgs{

					Term:         term,
					LeaderId:     rf.me,
					PrevLogIndex: prevIndex,
					PrevLogTerm:  prevTerm,
					Entries:      nil,
					LeaderCommit: commitIndex,
				}
				rf.mu.Unlock()

				reply := &AppendEntriesReply{}
				rf.sendAppendEntries(server, args, reply)
			}(i)
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

	// start the election ticker
	go rf.ticker()

	return rf
}
