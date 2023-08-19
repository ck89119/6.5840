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
	"sort"

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
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

func (rf *Raft) getLastLog() LogEntry {
	return rf.log[len(rf.log)-1]
}

func (rf *Raft) convertToFollower(term int, needPersist bool) {
	rf.currentTerm = term
	rf.state = FOLLOWER
	DPrintf("[%d] request/reply.Term > rf.currentTerm, convert to FOLLOWER\n", rf.me)
	if needPersist {
		rf.persist()
	}
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
	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	_ = encoder.Encode(rf.currentTerm)
	_ = encoder.Encode(rf.votedFor)
	_ = encoder.Encode(rf.log)
	DPrintf("[%d] persist() currentTerm = %v, votedFor = %v, len(log) = %v\n", rf.me, rf.currentTerm, rf.votedFor, len(rf.log))
	// TODO snapshot
	rf.persister.Save(writer.Bytes(), nil)
	DPrintf("[%d] persist() end\n", rf.me)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		DPrintf("[%d] readPersist data is empty\n", rf.me)
		return
	}

	reader := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(reader)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	_ = decoder.Decode(&currentTerm)
	_ = decoder.Decode(&votedFor)
	_ = decoder.Decode(&log)
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	DPrintf("[%d] readPersist() currentTerm = %v, votedFor = %v, len(log) = %v\n", rf.me, rf.currentTerm, rf.votedFor, len(rf.log))
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
	DPrintf("[%d]:[%d] receive RequestVote from %d, args = %v, votedFor = %v\n", rf.me, rf.currentTerm, args.CandidateId, args, rf.votedFor)
	reply.Term = rf.currentTerm

	if rf.currentTerm > args.Term {
		DPrintf("[%d]:[%d] RequestVote rf.currentTerm > args.Term", rf.me, rf.currentTerm)
		reply.VoteGranted = false
		return
	}

	if rf.currentTerm == args.Term && rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		DPrintf("[%d]:[%d] RequestVote rf.votedFor != args.CandidateId", rf.me, rf.currentTerm)
		reply.VoteGranted = false
		return
	}

	needPersist := false
	if rf.currentTerm < args.Term {
		rf.convertToFollower(args.Term, false)
		needPersist = true
	}

	lastLog := rf.getLastLog()
	DPrintf("[%d]:[%d] lastLog.Term = %d, lastLog.Index = %d\n", rf.me, rf.currentTerm, lastLog.Term, lastLog.Index)
	if lastLog.Term > args.LastLogTerm || lastLog.Term == args.LastLogTerm && lastLog.Index > args.LastLogIndex {
		DPrintf("[%d]:[%d] log not match\n", rf.me, rf.currentTerm)
		reply.VoteGranted = false
		if needPersist {
			rf.persist()
		}
		return
	}

	rf.lastHeartBeat = time.Now()
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	rf.persist()
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.state = CANDIDATE
	rf.lastHeartBeat = time.Now()
	rf.currentTerm++
	rf.votedFor = rf.me
	term := rf.currentTerm
	lastLog := rf.getLastLog()
	rf.persist()
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
			DPrintf("[%d]:[%d] start Raft.RequestVote to: %d\n", rf.me, term, server)
			ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
			DPrintf("[%d]:[%d] finish Raft.RequestVote to: %d\n", rf.me, term, server)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.currentTerm < reply.Term {
				rf.convertToFollower(reply.Term, true)
			}

			if rf.state != CANDIDATE || rf.currentTerm != term {
				DPrintf("[%d]:[%d] not candidate any more for this term, quit election\n", rf.me, term)
				return
			}

			if !reply.VoteGranted {
				return
			}
			DPrintf("[%d] get vote from %d: for term %d\n", rf.me, server, term)

			votes++
			if !done && votes > len(rf.peers)/2 {
				DPrintf("[%d]:[%d] become leader\n", rf.me, term)

				rf.state = LEADER
				// re-initial nextIndex and matchIndex
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = len(rf.log)
					rf.matchIndex[i] = 0
				}
				rf.appendEntries(true)

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
				DPrintf("[%d]:[%d] send heart beat to: %d\n", rf.me, rf.currentTerm, server)
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
	return fmt.Sprintf("{Term = %d, LeaderId = %d, PrevLogIndex = %d, PrevLogTerm = %d, len(enties) = %d, LeaderCommit = %d}",
		args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommit)
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}

func (reply *AppendEntriesReply) String() string {
	if reply.Success {
		return fmt.Sprintf("{Term = %d, Success = %v}", reply.Term, reply.Success)
	}
	return fmt.Sprintf("{Term = %d, Success = %v, XTerm = %d, XIndex = %d, XLen = %d}", reply.Term, reply.Success, reply.XTerm, reply.XIndex, reply.XLen)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[%d]:[%d] handle AppendEntries, args: %v\n", rf.me, rf.currentTerm, args)
	rf.lastHeartBeat = time.Now()
	reply.Term = rf.currentTerm

	if rf.currentTerm > args.Term {
		reply.Success = false
		return
	}

	if rf.currentTerm < args.Term {
		rf.convertToFollower(args.Term, true)
	}

	rf.state = FOLLOWER

	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("[%d]:[%d] PrevLogIndex or PrevLogTerm not match\n", rf.me, rf.currentTerm)
		reply.Success = false

		if args.PrevLogIndex < len(rf.log) {
			DPrintf("[%d]:[%d] log[PrevLogIndex].Term: %d\n", rf.me, rf.currentTerm, rf.log[args.PrevLogIndex].Term)
			reply.XTerm = rf.log[args.PrevLogIndex].Term
			for reply.XIndex = args.PrevLogIndex; rf.log[reply.XIndex].Term == reply.XTerm; reply.XIndex-- {
			}
			reply.XIndex++
			reply.XLen = -1
		} else {
			DPrintf("[%d]:[%d] len(log) = %d\n", rf.me, rf.currentTerm, len(rf.log))
			reply.Term = -1
			reply.XIndex = -1
			reply.XLen = len(rf.log)
		}
		return
	}

	DPrintf("[%d]:[%d] args.Entries: %v\n", rf.me, rf.currentTerm, args.Entries)
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
	if len(args.Entries) > 0 {
		rf.persist()
	}

	DPrintf("[%d]:[%d] args.LeaderCommit = %d, rf.commitIndex = %v, len(log) = %d\n", rf.me, rf.currentTerm, args.LeaderCommit, rf.commitIndex, len(rf.log))
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, len(rf.log)-1)
		rf.commitIndexChangedCond.Signal()
	}
	reply.Success = true
}

func (rf *Raft) fastBackUp(reply *AppendEntriesReply, log []LogEntry) int {
	// Case 1: leader doesn't have XTerm:
	// nextIndex = XIndex
	// 4 5 5
	// 4 6 6 (l)
	// Case 2: leader has XTerm:
	// nextIndex = leader's last entry for XTerm + 1
	// 4 4 4
	// 4 6 6 (l)
	// Case 3: follower's log is too short:
	// nextIndex = XLen
	// 4
	// 4 6 6 (l)
	if reply.XLen != -1 {
		return reply.XLen // Case 3
	} else {
		lowerBound := func(a []LogEntry, x int) int {
			return sort.Search(len(a), func(i int) bool {
				return a[i].Term >= x
			})
		}

		// leader contains XTerm
		if log[lowerBound(log, reply.XTerm)].Term == reply.XTerm {
			upperBound := func(a []LogEntry, x int) int {
				return sort.Search(len(a), func(i int) bool {
					return a[i].Term > x
				})
			}
			return upperBound(log, reply.XTerm) // Case 2
		} else {
			return reply.XIndex // Case 1
		}
	}
}

func (rf *Raft) updateIndexes(term int, index int, server int, log []LogEntry) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index > rf.matchIndex[server] {
		rf.nextIndex[server] = index + 1
		rf.matchIndex[server] = index

		DPrintf("[%d]:[%d] index = %v, commitIndex = %v, nextIndex = %v, matchIndex = %v\n",
			rf.me, term, index, rf.commitIndex, rf.nextIndex, rf.matchIndex)
		for n := index; n > rf.commitIndex; n-- {
			DPrintf("log[%d].term = %v, term = %v\n", n, log[n].Term, term)
			if log[n].Term != term {
				continue
			}

			count := 1
			for i, matchIndex := range rf.matchIndex {
				if i == rf.me {
					continue
				}
				if matchIndex >= n {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				DPrintf("[%d]:[%d] find n = %d bigger than commitIndex(%d) with majority\n", rf.me, term, n, rf.commitIndex)
				rf.commitIndex = index
				rf.commitIndexChangedCond.Signal()
				break
			}
		}
	}
}

func (rf *Raft) appendEntriesWithRetry(server int, term int, log []LogEntry, index int, commitIndex int, nextIndex int) {
	// retry until success
	for {
		rf.mu.Lock()
		if rf.state != LEADER || rf.currentTerm != term {
			DPrintf("[%d] not Leader any more\n", rf.me)
			rf.mu.Unlock()
			return
		}

		prevLog := log[nextIndex-1]
		entries := append(make([]LogEntry, 0), log[nextIndex:index+1]...)
		args := AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: prevLog.Index,
			PrevLogTerm:  prevLog.Term,
			Entries:      entries,
			LeaderCommit: commitIndex,
		}
		reply := AppendEntriesReply{}
		rf.mu.Unlock()

		DPrintf("[%d]:[%d] start Raft.AppendEntries to: %d\n", rf.me, term, server)
		ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
		DPrintf("[%d]:[%d] finish Raft.AppendEntries to: %d, ok = %v, reply = %s\n", rf.me, term, server, ok, &reply)
		if !ok {
			return
		}

		if reply.Success {
			break
		}

		rf.mu.Lock()
		if rf.currentTerm < reply.Term {
			rf.convertToFollower(reply.Term, true)
		}

		if rf.state != LEADER || rf.currentTerm != term {
			DPrintf("[%d] not Leader any more\n", rf.me)
			rf.mu.Unlock()
			return
		}

		//newNextIndex := rf.fastBackUp(&reply, log)
		//DPrintf("[%d]:[%d] newNextIndex = %d, nextIndex[server] = %d\n", rf.me, term, newNextIndex, rf.nextIndex[server])
		//if newNextIndex >= rf.nextIndex[server] {
		//	DPrintf("[%d]:[%d] newNextIndex(%d) >= nextIndex[server](%d), not expected\n", rf.me, term, newNextIndex, rf.nextIndex[server])
		//}
		//rf.nextIndex[server] = newNextIndex

		// if failed, nextIndex decrease only
		rf.nextIndex[server] = Min(rf.nextIndex[server], rf.fastBackUp(&reply, log))
		nextIndex = rf.nextIndex[server]

		if rf.nextIndex[server] <= 0 {
			DPrintf("[%d]:[%d] nextIndex[%d] <= 0, not expected\n", rf.me, term, server)
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}

	// if reply Success, reach here
	rf.updateIndexes(term, index, server, log)
}

func (rf *Raft) appendEntries(heartBeat bool) {
	// term do not change
	term := rf.currentTerm

	log := rf.log
	index := len(log) - 1
	commitIndex := rf.commitIndex

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		if !heartBeat && rf.nextIndex[i] > index {
			continue
		}

		nextIndex := rf.nextIndex[i]
		go func(server int, log []LogEntry, index int, commitIndex int) {
			for rf.killed() == false {

				go rf.appendEntriesWithRetry(server, term, log, index, commitIndex, nextIndex)

				// if not heartBeat, quit
				if !heartBeat {
					return
				}

				// 10 times heart beat per second
				time.Sleep(100 * time.Millisecond)
				//DPrintf("[%d]:[%d] sleep end\n", rf.me, term)

				rf.mu.Lock()
				if rf.state != LEADER || rf.currentTerm != term {
					DPrintf("[%d] not Leader any more, stop heartBeat\n", rf.me)
					rf.mu.Unlock()
					return
				}

				// heartBeat Call need the newest info
				// no need to update term, if currentTerm != oldTerm, then this long run heartBeat need to stop
				//term = rf.currentTerm
				log = rf.log
				index = len(log) - 1
				commitIndex = rf.commitIndex
				nextIndex = rf.nextIndex[server]
				rf.mu.Unlock()
			}
		}(i, log, index, commitIndex)
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
		DPrintf("[%d]:[%d] is not leader\n", rf.me, rf.currentTerm)
		return -1, -1, false
	}

	DPrintf("[%d]:[%d] receive command: %v\n", rf.me, rf.currentTerm, command)
	index := len(rf.log)
	term := rf.currentTerm
	rf.log = append(rf.log, LogEntry{Term: term, Index: index, Command: command})
	rf.persist()
	rf.appendEntries(false)
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
	for rf.killed() == false {
		rf.commitIndexChangedCond.Wait()
		DPrintf("[%d] awake\n", rf.me)

		rf.mu.Lock()
		commitIndex := rf.commitIndex
		log := rf.log
		rf.mu.Unlock()

		DPrintf("[%d] commitIndex = %d, prevCommitIndex = %d\n", rf.me, commitIndex, prevCommitIndex)
		if commitIndex > prevCommitIndex {
			for i := prevCommitIndex + 1; i <= commitIndex; i++ {
				rf.applyCh <- ApplyMsg{CommandValid: true, Command: log[i].Command, CommandIndex: i}
				DPrintf("[%d] push message to applyCh, command = %v\n", rf.me, log[i].Command)
			}
			prevCommitIndex = commitIndex
		}
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.sendToApplyCh()

	return rf
}
