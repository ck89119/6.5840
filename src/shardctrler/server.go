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

type Result struct {
	Seq    int64
	Err    Err
	Config Config
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

	lastResult map[int64]Result
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

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	lastResult := sc.getLastResult(args.ClientId)
	if lastResult.Seq == args.Seq {
		DPrintf("[%d] duplicate Join, args = %s\n", sc.me, args)
		reply.Err = lastResult.Err
		return
	}

	err := sc.waitForApplied(Op{
		Type: "Join",
		Data: *args,
	})

	reply.Err = err
}

func (sc *ShardCtrler) applyJoin(args *JoinArgs) {
	DPrintf("[%d] start applyJoin, args = %s\n", sc.me, args)

	lastResult := sc.getLastResult(args.ClientId)
	if lastResult.Seq == args.Seq {
		DPrintf("[%d] duplicate Join, args = %s\n", sc.me, args)
		return
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()
	clone := sc.configs[len(sc.configs)-1].Clone()
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

	sc.lastResult[args.ClientId] = Result{
		Seq: args.Seq,
		Err: OK,
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	lastResult := sc.getLastResult(args.ClientId)
	if lastResult.Seq == args.Seq {
		DPrintf("[%d] duplicate Leave, args = %s\n", sc.me, args)
		reply.Err = lastResult.Err
		return
	}

	err := sc.waitForApplied(Op{
		Type: "Leave",
		Data: *args,
	})

	reply.Err = err
}

func (sc *ShardCtrler) applyLeave(args *LeaveArgs) {
	lastResult := sc.getLastResult(args.ClientId)
	if lastResult.Seq == args.Seq {
		DPrintf("[%d] duplicate Leave, args = %s\n", sc.me, args)
		return
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	clone := sc.configs[len(sc.configs)-1].Clone()
	for _, gid := range args.GIDs {
		delete(clone.Groups, gid)
	}
	(&clone).balance()
	sc.configs = append(sc.configs, clone)

	sc.lastResult[args.ClientId] = Result{
		Seq: args.Seq,
		Err: OK,
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	lastResult := sc.getLastResult(args.ClientId)
	if lastResult.Seq == args.Seq {
		DPrintf("[%d] duplicate Move, args = %s\n", sc.me, args)
		reply.Err = lastResult.Err
		return
	}

	err := sc.waitForApplied(Op{
		Type: "Move",
		Data: *args,
	})

	reply.Err = err
}

func (sc *ShardCtrler) applyMove(args *MoveArgs) {
	lastResult := sc.getLastResult(args.ClientId)
	if lastResult.Seq == args.Seq {
		DPrintf("[%d] duplicate Move, args = %s\n", sc.me, args)
		return
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	clone := sc.configs[len(sc.configs)-1].Clone()
	clone.Shards[args.Shard] = args.GID
	sc.configs = append(sc.configs, clone)

	sc.lastResult[args.ClientId] = Result{
		Seq: args.Seq,
		Err: OK,
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	lastResult := sc.getLastResult(args.ClientId)
	if lastResult.Seq == args.Seq {
		DPrintf("[%d] duplicate Query, args = %s\n", sc.me, args)
		reply.Err = lastResult.Err
		return
	}

	err := sc.waitForApplied(Op{
		Type: "Query",
		Data: *args,
	})
	DPrintf("[%d] err = %v\n", sc.me, err)

	reply.Err = err
	if err == OK {
		reply.Config = sc.getLastResult(args.ClientId).Config
	}
}

func (sc *ShardCtrler) applyQuery(args *QueryArgs) {
	lastResult := sc.getLastResult(args.ClientId)
	if lastResult.Seq == args.Seq {
		DPrintf("[%d] duplicate Query, args = %s\n", sc.me, args)
		return
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	if args.Num == -1 {
		args.Num = len(sc.configs) - 1
	}
	sc.lastResult[args.ClientId] = Result{
		Seq:    args.Seq,
		Err:    OK,
		Config: sc.configs[args.Num],
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
			args := op.Data.(JoinArgs)
			sc.applyJoin(&args)
		} else if op.Type == "Leave" {
			args := op.Data.(LeaveArgs)
			sc.applyLeave(&args)
		} else if op.Type == "Move" {
			args := op.Data.(MoveArgs)
			sc.applyMove(&args)
		} else if op.Type == "Query" {
			args := op.Data.(QueryArgs)
			sc.applyQuery(&args)
		}

		sc.mu.Lock()
		sc.lastApplied = msg.CommandIndex
		sc.lastAppliedCond.Broadcast()
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) getLastResult(clientId int64) Result {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.lastResult[clientId]
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

	sc.lastResult = map[int64]Result{}

	go sc.apply()

	return sc
}
