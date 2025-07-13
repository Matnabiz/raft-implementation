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
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesReply
type AppendEntriesReply struct {
	Term    int
	Success bool
	NextTry int
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
		reply.NextTry = len(rf.log)
		return
	}
	// become follower
	rf.state = Follower
	rf.currentTerm = args.Term
	rf.electionReset = time.Now()

	// consistency check
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.NextTry = len(rf.log)
		return
	}
	// append entries
	rf.log = rf.log[:args.PrevLogIndex+1]
	rf.log = append(rf.log, args.Entries...)
	reply.Success = true

	// update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		go rf.applyEntries()
	}
}

// replicateLog sends AppendEntries to all followers
func (rf *Raft) replicateLog() {
	rf.mu.Lock()
	term := rf.currentTerm
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			prev := rf.nextIndex[server] - 1
			entries := make([]LogEntry, len(rf.log[prev+1:]))
			copy(entries, rf.log[prev+1:])
			args := &AppendEntriesArgs{
				Term:         term,
				LeaderId:     rf.me,
				PrevLogIndex: prev,
				PrevLogTerm:  rf.log[prev].Term,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()

			reply := &AppendEntriesReply{}
			if rf.sendAppendEntries(server, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Success {
					rf.matchIndex[server] = prev + len(entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1
					// advance commitIndex
					for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
						count := 1
						for j := range rf.peers {
							if j != rf.me && rf.matchIndex[j] >= N && rf.log[N].Term == rf.currentTerm {
								count++
							}
						}

						if count > len(rf.peers)/2 {
							rf.commitIndex = N
							go rf.applyEntries()
							break
						}
					}
				} else if reply.Term > rf.currentTerm {
					rf.state = Follower
					rf.currentTerm = reply.Term
					rf.votedFor = -1
				} else {
					rf.nextIndex[server] = reply.NextTry
				}
			}
		}(i)
	}
}

// applyEntries sends committed logs to applyCh
func (rf *Raft) applyEntries() {
	rf.mu.Lock()
	for rf.lastApplied < rf.commitIndex {
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

		// send empty AppendEntries as heartbeats
		rf.mu.Lock()
		term := rf.currentTerm
		rf.mu.Unlock()

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(server int) {
				rf.mu.Lock()
				prevIndex := rf.nextIndex[server] - 1
				args := &AppendEntriesArgs{
					Term:         term,
					LeaderId:     rf.me,
					PrevLogIndex: prevIndex,
					PrevLogTerm:  rf.log[prevIndex].Term,
					Entries:      nil, // heartbeat
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock()

				reply := &AppendEntriesReply{}
				rf.sendAppendEntries(server, args, reply)
			}(i)
		}

		// wait before next heartbeat
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
		log:           []LogEntry{{Term: 0}}, // dummy entry so log[0] exists
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
