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

func (msg ApplyMsg) String() string {
	if msg.CommandValid {
		return fmt.Sprintf("{CommandValid = %v, Command = %v, CommandIndex = %v}",
			msg.CommandValid, msg.Command, msg.CommandIndex)
	} else {
		return fmt.Sprintf("{SnapshotValid = %v, len(Snapshot) = %v, SnapshotTerm = %v, SnapshotIndex = %v}",
			msg.SnapshotValid, len(msg.Snapshot), msg.SnapshotTerm, msg.SnapshotIndex)
	}
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
	return fmt.Sprintf("{Term = %d, Index = %d, Command = %v}", entry.Term, entry.Index, entry.Command)
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

	snapshot      []byte
	snapshotTerm  int
	snapshotIndex int

	// Volatile state on all servers:
	commitIndex     int
	lastApplied     int
	lastAppliedCond *sync.Cond

	// Volatile state on leaders:
	// (Reinitialized after election)
	nextIndex  []int
	matchIndex []int

	applyCh chan ApplyMsg
}

func (rf *Raft) lowerBound(term int) int {
	return sort.Search(len(rf.log), func(i int) bool {
		return rf.log[i].Term >= term
	})
}

func (rf *Raft) upperBound(term int) int {
	return sort.Search(len(rf.log), func(i int) bool {
		return rf.log[i].Term > term
	})
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
	// with rf.mu.Locked
	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	_ = encoder.Encode(rf.currentTerm)
	_ = encoder.Encode(rf.votedFor)
	_ = encoder.Encode(rf.log)
	_ = encoder.Encode(rf.snapshotTerm)
	_ = encoder.Encode(rf.snapshotIndex)
	DPrintf("[%d] persist() currentTerm = %v, votedFor = %v, len(log) = %v, snapshotTerm = %d, snapshotIndex = %d\n",
		rf.me, rf.currentTerm, rf.votedFor, len(rf.log), rf.snapshotTerm, rf.snapshotIndex)
	// TODO unlock before write
	//rf.mu.Unlock()
	rf.persister.Save(writer.Bytes(), rf.snapshot)
	DPrintf("[%d] persist() end\n", rf.me)
	//rf.mu.Lock()
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
	var snapshotTerm int
	var snapshotIndex int
	_ = decoder.Decode(&currentTerm)
	_ = decoder.Decode(&votedFor)
	_ = decoder.Decode(&log)
	_ = decoder.Decode(&snapshotTerm)
	_ = decoder.Decode(&snapshotIndex)
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	rf.snapshotTerm = snapshotTerm
	rf.snapshotIndex = snapshotIndex
	DPrintf("[%d] readPersist() currentTerm = %v, votedFor = %v, len(log) = %v, snapshotTerm = %d, snapshotIndex = %d\n",
		rf.me, rf.currentTerm, rf.votedFor, len(rf.log), rf.snapshotTerm, rf.snapshotIndex)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

func (args *InstallSnapshotArgs) String() string {
	return fmt.Sprintf("{Term = %d, LeaderId = %d, LastIncludedIndex = %d, LastIncludedTerm = %d}",
		args.Term, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) installSnapshot(server int, term int) {
	// enter with locked
	args := InstallSnapshotArgs{
		Term:              term,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.snapshotIndex,
		LastIncludedTerm:  rf.snapshotTerm,
		Data:              rf.snapshot,
	}
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()

	DPrintf("[%d]:[%d] start Raft.InstallSnapshot to: %d, args: %s\n", rf.me, term, server, &args)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
	DPrintf("[%d]:[%d] finish Raft.InstallSnapshot to: %d, args: %s, ok: %v\n", rf.me, term, server, &args, ok)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm < reply.Term {
		rf.convertToFollower(reply.Term, true)
	}

	if rf.state != LEADER || rf.currentTerm != term {
		DPrintf("[%d] not Leader any more, stop heartBeat\n", rf.me)
		return
	}

	// if success, update indexes
	// no need to update commitIndex, because the snapshoted log must be applied/committed before
	rf.updateIndexes(server, term, args.LastIncludedIndex)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	DPrintf("[%d]:[%d] handle InstallSnapshot, args: %v\n", rf.me, rf.currentTerm, args)
	rf.lastHeartBeat = time.Now()
	reply.Term = rf.currentTerm

	if rf.currentTerm > args.Term {
		rf.mu.Unlock()
		return
	}

	needPersist := false
	if rf.currentTerm < args.Term {
		rf.convertToFollower(args.Term, false)
		needPersist = true
	}
	rf.state = FOLLOWER

	if rf.lastApplied >= args.LastIncludedIndex {
		DPrintf("[%d]:[%d] local status(rf.snapshotIndex = %d, rf.lastApplied = %d) is newer than the received snapshot, ignore\n",
			rf.me, rf.currentTerm, rf.snapshotIndex, rf.lastApplied)
		if needPersist {
			rf.persist()
		}
		rf.mu.Unlock()
		return
	}

	index := Min(args.LastIncludedIndex-rf.snapshotIndex, len(rf.log)-1)
	log := append(make([]LogEntry, 0), rf.log[index:]...)
	// if LastIncludedIndex > lastLog.Index, reset first log entry manually
	log[0].Term = args.LastIncludedTerm
	log[0].Index = args.LastIncludedIndex

	// update commitIndex and lastApplied
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex

	rf.log = log
	rf.snapshot = args.Data
	rf.snapshotTerm = args.LastIncludedTerm
	rf.snapshotIndex = args.LastIncludedIndex
	rf.persist()
	rf.mu.Unlock()

	go func(currentTerm int) {
		msg := ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
		rf.applyCh <- msg
		DPrintf("[%d]:[%d] push message to applyCh, msg = %v\n", rf.me, currentTerm, msg)
	}(rf.currentTerm)
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[%d]:[%d] start Snapshot, index = %d, len(snapshot) = %d\n", rf.me, rf.currentTerm, index, len(snapshot))

	if index <= rf.snapshotIndex || index > rf.getLastLog().Index {
		DPrintf("[%d]:[%d] index(%d) <= log[0].Index(%d) or > rf.getLastLog().Index(%d), do nothing\n",
			rf.me, rf.currentTerm, index, rf.log[0].Index, rf.getLastLog().Index)
		return
	}

	index -= rf.snapshotIndex
	snapshotTerm := rf.log[index].Term
	snapshotIndex := rf.log[index].Index
	log := append(make([]LogEntry, 0), rf.log[index:]...)

	rf.log = log
	rf.snapshot = snapshot
	rf.snapshotTerm = snapshotTerm
	rf.snapshotIndex = snapshotIndex
	rf.persist()
	DPrintf("[%d]:[%d] end Snapshot\n", rf.me, rf.currentTerm)
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
					rf.nextIndex[i] = rf.getLastLog().Index + 1
					rf.matchIndex[i] = rf.snapshotIndex
				}
				go rf.heartBeat()

				done = true
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
	return fmt.Sprintf("{Term = %d, LeaderId = %d, PrevLogIndex = %d, PrevLogTerm = %d, len(entries) = %d, LeaderCommit = %d}",
		args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommit)
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// XTerm: term in the conflicting entry (if any)
	XTerm int
	// XIndex: index of first entry with that term (if any)
	XIndex int
	// XLen: log length
	XLen int
}

func (reply *AppendEntriesReply) String() string {
	if reply.Success {
		return fmt.Sprintf("{Term = %d, Success = %v}", reply.Term, reply.Success)
	}
	return fmt.Sprintf("{Term = %d, Success = %v, XTerm = %d, XIndex = %d, XLen = %d}", reply.Term, reply.Success, reply.XTerm, reply.XIndex, reply.XLen)
}

func (rf *Raft) applyCommand() {
	DPrintf("[%d] commitIndex = %d, lastApplied = %d\n", rf.me, rf.commitIndex, rf.lastApplied)
	if rf.commitIndex <= rf.lastApplied {
		return
	}

	// send log[lastApplied + 1 : commit] to applyCh
	startIndex := Max(0, rf.lastApplied-rf.snapshotIndex+1)
	endIndex := rf.commitIndex - rf.snapshotIndex
	DPrintf("[%d] startIndex = %d, endIndex = %d\n", rf.me, startIndex, endIndex)
	log := append(make([]LogEntry, 0), rf.log[startIndex:endIndex+1]...)

	go func() {
		rf.lastAppliedCond.L.Lock()
		for _, logEntry := range log {
			for logEntry.Index > rf.lastApplied+1 {
				rf.lastAppliedCond.Wait()
			}
			if logEntry.Index != rf.lastApplied+1 {
				continue
			}

			msg := ApplyMsg{CommandValid: true, Command: logEntry.Command, CommandIndex: logEntry.Index}
			DPrintf("[%d] start push message to applyCh, msg = %s\n", rf.me, msg)
			rf.applyCh <- msg
			DPrintf("[%d] end push message to applyCh, msg = %s\n", rf.me, msg)

			rf.mu.Lock()
			rf.lastApplied = Max(rf.lastApplied, logEntry.Index)
			rf.mu.Unlock()
			rf.lastAppliedCond.Signal()
		}
		rf.lastAppliedCond.L.Unlock()
		DPrintf("[%d] push message end\n", rf.me)
	}()
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

	needPersist := false
	if rf.currentTerm < args.Term {
		rf.convertToFollower(args.Term, false)
		needPersist = true
	}
	rf.state = FOLLOWER

	args.PrevLogIndex -= rf.snapshotIndex
	if args.PrevLogIndex >= len(rf.log) {
		DPrintf("[%d]:[%d] follower's log is too short, PrevLogIndex = %d, len(rf.log) = %d\n",
			rf.me, rf.currentTerm, args.PrevLogIndex, len(rf.log))

		reply.Success = false
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = rf.getLastLog().Index + 1

		if needPersist {
			rf.persist()
		}
		return
	}

	if args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("[%d]:[%d] PrevLogTerm not match, args.PrevLogTerm = %d, rf.log[args.PrevLogIndex].Term = %d\n",
			rf.me, rf.currentTerm, args.PrevLogTerm, rf.log[args.PrevLogIndex].Term)

		reply.Success = false
		reply.XTerm = rf.log[args.PrevLogIndex].Term
		reply.XIndex = rf.lowerBound(reply.XTerm) + rf.snapshotIndex
		reply.XLen = -1

		if needPersist {
			rf.persist()
		}
		return
	}

	DPrintf("[%d]:[%d] args.Entries: %v\n", rf.me, rf.currentTerm, args.Entries)
	for _, newEntry := range args.Entries {
		logIndex := newEntry.Index - rf.snapshotIndex
		if logIndex <= 0 {
			continue
		}

		if logIndex < len(rf.log) {
			if rf.log[logIndex].Term == newEntry.Term {
				continue
			}
			// if don't match, delete this entry and all followed it
			rf.log = rf.log[:logIndex]
		}

		if len(rf.log) != logIndex {
			DPrintf("WARNING: [%d] newEntry's index: %d does not match the actual location: %d\n", rf.me, newEntry.Index, len(rf.log)+rf.snapshotIndex)
		}
		rf.log = append(rf.log, newEntry)
		needPersist = true
	}

	DPrintf("[%d]:[%d] args.LeaderCommit = %d, rf.commitIndex = %v, rf.getLastLog().Index = %d\n", rf.me, rf.currentTerm, args.LeaderCommit, rf.commitIndex, rf.getLastLog().Index)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, rf.getLastLog().Index)
		rf.applyCommand()
	}
	reply.Success = true

	if needPersist {
		rf.persist()
	}
}

func (rf *Raft) fastBackUp(reply *AppendEntriesReply) int {
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
		index := rf.lowerBound(reply.XTerm)
		// leader contains XTerm
		if index < len(rf.log) && rf.log[index].Term == reply.XTerm {
			return rf.upperBound(reply.XTerm) + rf.snapshotIndex // Case 2
		} else {
			return reply.XIndex // Case 1
		}
	}
}

func (rf *Raft) updateIndexes(server int, term int, lastMatchedIndex int) {
	DPrintf("[%d]:[%d] lastMatchedIndex = %v, rf.matchIndex[%d] = %v\n",
		rf.me, rf.currentTerm, lastMatchedIndex, server, rf.matchIndex[server])

	if lastMatchedIndex > rf.matchIndex[server] {
		rf.nextIndex[server] = lastMatchedIndex + 1
		rf.matchIndex[server] = lastMatchedIndex

		DPrintf("[%d]:[%d] lastMatchedIndex = %v, commitIndex = %v, nextIndex = %v, matchIndex = %v\n",
			rf.me, term, lastMatchedIndex, rf.commitIndex, rf.nextIndex, rf.matchIndex)
		for n := lastMatchedIndex; n > rf.commitIndex; n-- {
			if rf.log[n-rf.snapshotIndex].Term != term {
				DPrintf("[%d]:[%d] rf.log[%d].Term = %v\n", rf.me, term, n, rf.log[n-rf.snapshotIndex].Term)
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
				rf.commitIndex = n
				rf.applyCommand()
				break
			}
		}
	}
}

func (rf *Raft) appendEntries(server int, term int) {
	// enter with locked
	prevLog := rf.log[rf.nextIndex[server]-rf.snapshotIndex-1]
	startIndex := rf.nextIndex[server] - rf.snapshotIndex
	//endIndex := rf.getLastLog().Index - rf.snapshotIndex
	entries := append(make([]LogEntry, 0), rf.log[startIndex:]...)
	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: prevLog.Index,
		PrevLogTerm:  prevLog.Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	reply := AppendEntriesReply{}
	rf.mu.Unlock()

	DPrintf("[%d]:[%d] start Raft.AppendEntries to: %d, args = %s\n", rf.me, term, server, &args)
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	DPrintf("[%d]:[%d] finish Raft.AppendEntries to: %d, ok = %v, args = %s, reply = %s\n", rf.me, term, server, ok, &args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm < reply.Term {
		rf.convertToFollower(reply.Term, true)
	}

	if rf.state != LEADER || rf.currentTerm != term {
		DPrintf("[%d] not Leader any more, stop heartBeat\n", rf.me)
		return
	}

	if !reply.Success {
		// if failed, nextIndex decrease only
		rf.nextIndex[server] = Min(rf.nextIndex[server], rf.fastBackUp(&reply))
	} else if len(entries) > 0 {
		// if success and not an empty heart beat, update indexes
		rf.updateIndexes(server, term, entries[len(entries)-1].Index)
	}
}

func (rf *Raft) heartBeat() {
	term := rf.currentTerm

	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state != LEADER || rf.currentTerm != term {
			DPrintf("[%d] not Leader any more, stop heartBeat\n", rf.me)
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			go func(server int) {
				// unlock in called function
				rf.mu.Lock()

				if rf.nextIndex[server] <= rf.snapshotIndex {
					//DPrintf("[%d]:[%d] unexpected here, rf.nextIndex[%d] = %d, rf.snapshotIndex = %d\n", rf.me, term, server, rf.nextIndex[server], rf.snapshotIndex)
					rf.installSnapshot(server, term)
				} else {
					rf.appendEntries(server, term)
				}
			}(i)
		}

		DPrintf("[%d]:[%d] heartBeat\n", rf.me, term)
		// 10 times heart beat per second
		time.Sleep(100 * time.Millisecond)
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
	index := rf.getLastLog().Index + 1
	term := rf.currentTerm
	rf.log = append(rf.log, LogEntry{Term: term, Index: index, Command: command})
	rf.persist()
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

func (rf *Raft) electionTicker() {
	for rf.killed() == false {
		// pause for a random amount of time between 100 and 400
		// milliseconds.
		ms := 200 + (rand.Int63() % 200)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		rf.mu.Lock()
		// Check if a leader election should be started.
		duration := time.Now().Sub(rf.lastHeartBeat)
		DPrintf("[%d]:[%d] state: %v, duration from last heartBeaten: %v (ms)\n", rf.me, rf.currentTerm, rf.state, duration)
		if rf.state != LEADER && duration > time.Duration(ms)*time.Millisecond {
			go rf.startElection()
		}

		rf.mu.Unlock()
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

	// non-volatile
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{{0, 0, nil}}
	rf.snapshot = nil
	rf.snapshotTerm = 0
	rf.snapshotIndex = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.commitIndex = rf.snapshotIndex
	rf.lastApplied = rf.snapshotIndex
	rf.lastAppliedCond = sync.NewCond(&sync.Mutex{})

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.getLastLog().Index + 1
		rf.matchIndex[i] = rf.snapshotIndex
	}

	rf.applyCh = applyCh

	// start ticker goroutine to start elections
	go rf.electionTicker()

	return rf
}
