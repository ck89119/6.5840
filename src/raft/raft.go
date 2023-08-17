package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isLeader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

type State string

const FOLLOWER State = "Follower"
const CANDIDATE State = "Candidate"
const LEADER State = "Leader"

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

func (entry LogEntry) String() string {
	return fmt.Sprintf("{Term = %d, Index = %d, command = %v}", entry.Term, entry.Index, entry.Command)
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state         State
	lastHeartBeat time.Time

	// Persistent state on all servers:
	// (Updated on stable storage before responding to RPCs)
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers:
	commitIndex            int
	lastApplied            int
	commitIndexChangedCond *sync.Cond

	// Volatile state on leaders:
	// (Reinitialized after election)
	nextIndex  []int
	matchIndex []int

	applyCh chan ApplyMsg
}

func (rf *Raft) changeStateToFollower(term int) {
	rf.mu.Lock()
	defer rf.mu.Lock()

	rf.currentTerm = term
	rf.state = FOLLOWER
}

func (rf *Raft) getLastLog() LogEntry {
	return rf.log[len(rf.log)-1]
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == LEADER
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

func (args *RequestVoteArgs) String() string {
	return fmt.Sprintf("{Term = %d, CandidateId = %d, LastLogIndex = %d, LastLogTerm = %d}", args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (2A, 2B).
	DPrintf("[%d] receive RequestVote from %d, args = %v, currentTerm = %v, votedFor = %v\n", rf.me, args.CandidateId, args, rf.currentTerm, rf.votedFor)
	reply.Term = rf.currentTerm

	if rf.currentTerm > args.Term {
		DPrintf("[%d] RequestVote rf.currentTerm > args.Term", rf.me)
		reply.VoteGranted = false
		return
	}

	if rf.currentTerm == args.Term && rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		DPrintf("[%d] RequestVote rf.votedFor != args.CandidateId", rf.me)
		reply.VoteGranted = false
		return
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
	}

	lastLog := rf.getLastLog()
	DPrintf("[%d] lastLog.Term = %d, lastLog.Index = %d\n", rf.me, lastLog.Term, lastLog.Index)
	if lastLog.Term > args.LastLogTerm || lastLog.Term == args.LastLogTerm && lastLog.Index > args.LastLogIndex {
		DPrintf("[%d] log not match\n", rf.me)
		reply.VoteGranted = false
		return
	}

	rf.lastHeartBeat = time.Now()
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.state = CANDIDATE
	rf.lastHeartBeat = time.Now()
	rf.currentTerm++
	rf.votedFor = rf.me
	term := rf.currentTerm
	lastLog := rf.getLastLog()
	rf.mu.Unlock()
	DPrintf("[%d] startElection at term: %d\n", rf.me, term)

	votes := 1
	done := false
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogIndex: lastLog.Index,
				LastLogTerm:  lastLog.Term,
			}
			reply := RequestVoteReply{}
			DPrintf("[%d] start Raft.RequestVote to: %d\n", rf.me, server)
			ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
			DPrintf("[%d] finish Raft.RequestVote to: %d\n", rf.me, server)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = FOLLOWER
				return
			}

			if !reply.VoteGranted {
				return
			}
			DPrintf("[%d] get vote from %d: for term %d\n", rf.me, server, term)

			if done || rf.state != CANDIDATE || rf.currentTerm != term {
				return
			}

			votes++
			if votes > len(rf.peers)/2 {
				DPrintf("[%d] become new leader\n", rf.me)

				rf.state = LEADER
				// re-initial nextIndex and matchIndex
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = len(rf.log)
					rf.matchIndex[i] = 0
				}
				rf.heartBeat()

				// param index: new log index, there's no new log here, so return the last log's index
				//rf.appendEntries(len(log)-1, term, commitIndex, log, true)
				done = true
			}
		}(i)
	}
}

func (rf *Raft) heartBeat() {
	term := rf.currentTerm

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			for {
				rf.mu.Lock()
				log := rf.log
				nextIndex := rf.nextIndex[server]
				prevLog := log[nextIndex-1]
				commitIndex := rf.commitIndex
				if rf.state != LEADER || rf.currentTerm != term {
					rf.mu.Unlock()
					break
				}
				rf.mu.Unlock()

				args := AppendEntriesArgs{
					Term:         term,
					LeaderId:     rf.me,
					PrevLogIndex: prevLog.Index,
					PrevLogTerm:  prevLog.Term,
					Entries:      log[nextIndex:],
					LeaderCommit: commitIndex,
				}
				reply := AppendEntriesReply{}
				DPrintf("[%d] send heart beat to: %d\n", rf.me, server)
				go func() {
					rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
				}()

				// 10 times heart beat per second
				time.Sleep(100 * time.Millisecond)
			}
		}(i)
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	//leader's commit index
	LeaderCommit int
}

func (args *AppendEntriesArgs) String() string {
	return fmt.Sprintf("{Term = %d, len(enties) = %d, LeaderCommit = %d}", args.Term, len(args.Entries), args.LeaderCommit)
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[%d] receive AppendEntries from: %d, currentTerm: %d, args: %v\n",
		rf.me, args.LeaderId, rf.currentTerm, args)
	rf.lastHeartBeat = time.Now()
	reply.Term = rf.currentTerm

	if rf.currentTerm > args.Term {
		reply.Success = false
		return
	}

	rf.currentTerm = args.Term

	//DPrintf("[%d] change state form %s to %s\n", rf.me, rf.state, FOLLOWER)
	rf.state = FOLLOWER

	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("[%d] PrevLogIndex or PrevLogTerm not match\n", rf.me)
		reply.Success = false
		return
	}

	DPrintf("[%d] args.Entries: %v\n", rf.me, args.Entries)
	for _, newEntry := range args.Entries {
		if newEntry.Index < len(rf.log) {
			if rf.log[newEntry.Index].Term == newEntry.Term {
				continue
			}
			// if don't match, delete this entry and all followed it
			rf.log = rf.log[:newEntry.Index]
		}

		if len(rf.log) != newEntry.Index {
			DPrintf("[%d] newEntry's index: %d does not match the actual location: %d\n", rf.me, newEntry.Index, len(rf.log))
		}
		rf.log = append(rf.log, newEntry)
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, len(rf.log)-1)
		rf.commitIndexChangedCond.Signal()
	}
	reply.Success = true
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) appendEntries(index, term, commitIndex int, log []LogEntry) {
	DPrintf("[%d] start appendEntries, index: %d, term: %d, commitIndex: %d, len(log): %d\n", rf.me, index, term, commitIndex, len(rf.log))

	count := 1
	done := false
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		if index < rf.nextIndex[i] {
			continue
		}

		nextIndex := rf.nextIndex[i]
		go func(server int) {
			for ; nextIndex > 0; nextIndex-- {
				prevLog := log[nextIndex-1]
				args := AppendEntriesArgs{
					Term:         term,
					LeaderId:     rf.me,
					PrevLogIndex: prevLog.Index,
					PrevLogTerm:  prevLog.Term,
					Entries:      log[nextIndex : index+1],
					LeaderCommit: commitIndex,
				}
				reply := AppendEntriesReply{}
				DPrintf("[%d] start Raft.AppendEntries to: %d\n", rf.me, server)
				ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
				DPrintf("[%d] finish Raft.AppendEntries to: %d\n", rf.me, server)
				if !ok {
					return
				}

				if reply.Success {
					break
				}

				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = FOLLOWER
					DPrintf("[%d] reply.Term > rf.currentTerm, convert to FOLLOWER\n", rf.me)
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
			}

			if nextIndex == 0 {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			rf.nextIndex[server] = Max(rf.nextIndex[server], index+1)
			rf.matchIndex[server] = Max(rf.nextIndex[server], index)

			count += 1
			if !done && count > len(rf.peers)/2 && rf.currentTerm == term {
				rf.commitIndex = Max(rf.commitIndex, index)
				rf.commitIndexChangedCond.Signal()
				done = true
			}
		}(i)
	}
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER {
		DPrintf("[%d] is not leader\n", rf.me)
		return -1, -1, false
	}

	DPrintf("[%d] receive command: %v\n", rf.me, command)
	index := len(rf.log)
	term := rf.currentTerm
	commitIndex := rf.commitIndex
	rf.log = append(rf.log, LogEntry{Term: term, Index: index, Command: command})
	log := rf.log

	rf.appendEntries(index, term, commitIndex, log)
	return index, term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// pause for a random amount of time between 100 and 400
		// milliseconds.
		ms := 200 + (rand.Int63() % 200)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()

		duration := time.Now().Sub(rf.lastHeartBeat)
		DPrintf("[%d] state: %v, duration from last heartBeaten: %v (ms)\n", rf.me, rf.state, duration)
		if rf.state != LEADER && duration > time.Duration(ms)*time.Millisecond {
			go rf.startElection()
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) sendToApplyCh() {
	prevCommitIndex := 0
	rf.commitIndexChangedCond.L.Lock()
	defer rf.commitIndexChangedCond.L.Unlock()
	for {
		rf.commitIndexChangedCond.Wait()
		DPrintf("[%d] awake\n", rf.me)

		rf.mu.Lock()
		commitIndex := rf.commitIndex
		log := rf.log
		rf.mu.Unlock()

		DPrintf("[%d] rf.commitIndex = %d, prevCommitIndex = %d\n", rf.me, commitIndex, prevCommitIndex)
		if commitIndex > prevCommitIndex {
			for i := prevCommitIndex + 1; i <= commitIndex; i++ {
				rf.applyCh <- ApplyMsg{CommandValid: true, Command: log[i].Command, CommandIndex: i}
				DPrintf("[%d] push message to applyCh, command = %v\n", rf.me, log[i].Command)
			}
			prevCommitIndex = commitIndex
		}
		DPrintf("[%d] finish sendToApplyCh\n", rf.me)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	DPrintf("me = %d\n", me)

	// Your initialization code here (2A, 2B, 2C).
	rf.state = FOLLOWER
	rf.lastHeartBeat = time.Now()

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{{0, 0, nil}}

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.commitIndexChangedCond = sync.NewCond(&sync.Mutex{})

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}

	rf.applyCh = applyCh
	go rf.sendToApplyCh()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
