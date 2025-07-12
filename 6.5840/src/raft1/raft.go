package raft

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *tester.Persister
	me        int
	dead      int32

	state         State
	currentTerm   int
	votedFor      int
	electionReset time.Time
}

type RequestVoteArgs struct {
	Term        int
	CandidateId int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Dummy implementation to satisfy the interface
	return -1, -1, false
}
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Dummy implementation for now
}
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}
func (rf *Raft) PersistBytes() int {
	return 0
}
func (rf *Raft) persist() {
	// Not needed for 3A
}

func (rf *Raft) readPersist(data []byte) {
	// Not needed for 3A
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
	}

	reply.Term = rf.currentTerm
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.electionReset = time.Now()
	} else {
		reply.VoteGranted = false
	}
}

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
	rf.votedFor = args.LeaderId
	rf.electionReset = time.Now()
	reply.Success = true
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		elapsed := time.Since(rf.electionReset)
		timeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
		rf.mu.Unlock()

		if state != Leader && elapsed >= timeout {
			go rf.startElection()
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm += 1
	term := rf.currentTerm
	rf.votedFor = rf.me
	rf.electionReset = time.Now()
	rf.mu.Unlock()

	var votes int32 = 1
	var wg sync.WaitGroup

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			args := &RequestVoteArgs{
				Term:        term,
				CandidateId: rf.me,
			}
			reply := &RequestVoteReply{}

			if rf.sendRequestVote(i, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.currentTerm {
					rf.state = Follower
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					return
				}
				if reply.VoteGranted && rf.currentTerm == term && rf.state == Candidate {
					atomic.AddInt32(&votes, 1)
				}
			}
		}(i)
	}

	wg.Wait()

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Candidate && int(votes) > len(rf.peers)/2 {
		rf.state = Leader
		go rf.sendHeartbeats()
	}
}

func (rf *Raft) sendHeartbeats() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		term := rf.currentTerm
		rf.mu.Unlock()

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(i int) {
				args := &AppendEntriesArgs{
					Term:     term,
					LeaderId: rf.me,
				}
				reply := &AppendEntriesReply{}
				rf.sendAppendEntries(i, args, reply)
			}(i)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

func Make(peers []*labrpc.ClientEnd, me int, persister *tester.Persister, applyCh chan raftapi.ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.state = Follower
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.electionReset = time.Now()

	go rf.ticker()

	return rf
}
