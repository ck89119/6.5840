package shardctrler

import (
	"6.5840/raft"
	"log"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs []Config // indexed by config num

	lastApplied     int
	lastAppliedCond *sync.Cond

	duplicateTable map[int64]int64
}

type Op struct {
	Type string
	Data interface{}
}

func (sc *ShardCtrler) waitForApplied(op Op) Err {
	index, term, isLeader := sc.rf.Start(op)
	DPrintf("[%d] index = %v, term = %v, isLeader = %v\n", sc.me, index, term, isLeader)
	if !isLeader {
		return ErrWrongLeader
	}

	sc.lastAppliedCond.L.Lock()
	defer sc.lastAppliedCond.L.Unlock()
	for sc.lastApplied < index {
		sc.lastAppliedCond.Wait()
	}

	appliedLogTerm := sc.rf.GetLogTerm(index)
	DPrintf("[%d]index = %v, appliedLogTerm = %v, term = %v\n", sc.me, index, appliedLogTerm, term)
	if appliedLogTerm != term {
		return ErrWrongTerm
	}

	return OK
}

func (sc *ShardCtrler) isDuplicatedRequest(clientId, seq int64) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return seq == sc.duplicateTable[clientId]
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	if sc.isDuplicatedRequest(args.ClientId, args.Seq) {
		reply.Err = OK
		return
	}

	err := sc.waitForApplied(Op{
		Type: "Join",
		Data: *args,
	})

	if err == ErrWrongLeader {
		reply.WrongLeader = true
	} else if err == ErrWrongTerm {
		reply.WrongLeader = false
		reply.Err = err
	} else {
		reply.WrongLeader = false
		reply.Err = OK
	}
}

func (sc *ShardCtrler) applyJoin(args JoinArgs) {
	DPrintf("[%d] start applyJoin, args = %s\n", sc.me, &args)

	if sc.isDuplicatedRequest(args.ClientId, args.Seq) {
		return
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()
	defer func() {
		sc.duplicateTable[args.ClientId] = args.Seq
	}()

	clone := sc.configs[len(sc.configs)-1].clone()
	DPrintf("[%d] start applyJoin, clone finished\n", sc.me)
	for gid, serverNames := range args.Servers {
		DPrintf("[%d] start applyJoin, gid = %v, serverNames = %v\n", sc.me, gid, serverNames)
		if servers, ok := clone.Groups[gid]; ok {
			servers = append(servers, serverNames...)
			continue
		}
		clone.Groups[gid] = serverNames
	}
	(&clone).balance()
	DPrintf("[%d] start applyJoin, balance finished\n", sc.me)
	sc.configs = append(sc.configs, clone)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	if sc.isDuplicatedRequest(args.ClientId, args.Seq) {
		reply.Err = OK
		return
	}

	err := sc.waitForApplied(Op{
		Type: "Leave",
		Data: *args,
	})

	if err == ErrWrongLeader {
		reply.WrongLeader = true
	} else if err == ErrWrongTerm {
		reply.WrongLeader = false
		reply.Err = err
	} else {
		reply.WrongLeader = false
		reply.Err = OK
	}
}

func (sc *ShardCtrler) applyLeave(args LeaveArgs) {
	if sc.isDuplicatedRequest(args.ClientId, args.Seq) {
		return
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()
	defer func() {
		sc.duplicateTable[args.ClientId] = args.Seq
	}()

	clone := sc.configs[len(sc.configs)-1].clone()
	for _, gid := range args.GIDs {
		delete(clone.Groups, gid)
	}
	(&clone).balance()
	sc.configs = append(sc.configs, clone)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	if sc.isDuplicatedRequest(args.ClientId, args.Seq) {
		reply.Err = OK
		return
	}

	err := sc.waitForApplied(Op{
		Type: "Move",
		Data: *args,
	})

	if err == ErrWrongLeader {
		reply.WrongLeader = true
	} else if err == ErrWrongTerm {
		reply.WrongLeader = false
		reply.Err = err
	} else {
		reply.WrongLeader = false
		reply.Err = OK
	}
}

func (sc *ShardCtrler) applyMove(args MoveArgs) {
	if sc.isDuplicatedRequest(args.ClientId, args.Seq) {
		return
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()
	defer func() {
		sc.duplicateTable[args.ClientId] = args.Seq
	}()

	clone := sc.configs[len(sc.configs)-1].clone()
	clone.Shards[args.Shard] = args.GID
	sc.configs = append(sc.configs, clone)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	err := sc.waitForApplied(Op{
		Type: "Query",
		Data: *args,
	})
	DPrintf("[%d] err = %v\n", sc.me, err)

	if err == ErrWrongLeader {
		reply.WrongLeader = true
	} else if err == ErrWrongTerm {
		reply.WrongLeader = false
		reply.Err = err
	} else {
		reply.WrongLeader = false
		reply.Err = OK
		sc.mu.Lock()
		if args.Num == -1 {
			args.Num = len(sc.configs) - 1
		}
		reply.Config = sc.configs[args.Num]
		sc.mu.Unlock()
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) apply() {
	for msg := range sc.applyCh {
		DPrintf("[%d] receive apply msg: %v\n", sc.me, msg)
		if msg.SnapshotValid {
			continue
		}

		op := msg.Command.(Op)
		if op.Type == "Join" {
			sc.applyJoin(op.Data.(JoinArgs))
		} else if op.Type == "Leave" {
			sc.applyLeave(op.Data.(LeaveArgs))
		} else if op.Type == "Move" {
			sc.applyMove(op.Data.(MoveArgs))
		}
		// do nothing for query and other unsupported op

		sc.mu.Lock()
		sc.lastApplied += 1
		sc.mu.Unlock()
		sc.lastAppliedCond.Broadcast()
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.lastApplied = 0
	sc.lastAppliedCond = sync.NewCond(&sc.mu)

	sc.duplicateTable = make(map[int64]int64)

	go sc.apply()

	return sc
}
