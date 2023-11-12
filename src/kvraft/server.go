package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Type     string // Get, Put or Append
	Key      string
	Value    string
	ClientId int64
	Seq      int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate            int // snapshot if log grows this big
	startIndex, lastApplied int
	lastAppliedCond         *sync.Cond

	table          map[string]string
	duplicateTable map[int64]int64
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("[%d] handle Get start, args = %s\n", kv.me, args)

	lastLogIndex, term, isLeader := kv.rf.Start(Op{
		Type: "Get",
		Key:  args.Key,
	})
	DPrintf("[%d] lastLogIndex = %v, term = %v, isLeader = %v\n", kv.me, lastLogIndex, term, isLeader)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.lastAppliedCond.L.Lock()
	defer kv.lastAppliedCond.L.Unlock()
	for kv.lastApplied < lastLogIndex {
		kv.lastAppliedCond.Wait()
	}

	appliedLogTerm := kv.rf.GetLogTerm(lastLogIndex)
	if appliedLogTerm != term {
		DPrintf("[%d] appliedLogTerm = %v, term = %v\n", kv.me, appliedLogTerm, term)
		reply.Err = ErrWrongTerm
		return
	}

	if value, ok := kv.table[args.Key]; ok {
		reply.Err = OK
		reply.Value = value
	} else {
		reply.Err = ErrNoKey
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("[%d] handle PutAppend start, args = %s\n", kv.me, args)

	kv.mu.Lock()
	if args.Seq == kv.duplicateTable[args.ClientId] {
		DPrintf("[%d] duplicate PutAppend, args = %s\n", kv.me, args)
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	lastLogIndex, term, isLeader := kv.rf.Start(Op{
		Type:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		Seq:      args.Seq,
	})
	DPrintf("[%d] lastLogIndex = %v, term = %v, isLeader = %v\n", kv.me, lastLogIndex, term, isLeader)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.lastAppliedCond.L.Lock()
	defer kv.lastAppliedCond.L.Unlock()
	for kv.lastApplied < lastLogIndex {
		kv.lastAppliedCond.Wait()
	}

	appliedLogTerm := kv.rf.GetLogTerm(lastLogIndex)
	if appliedLogTerm != term {
		DPrintf("[%d] appliedLogTerm = %v, term = %v\n", kv.me, appliedLogTerm, term)
		reply.Err = ErrWrongTerm
		return
	}

	reply.Err = OK
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) apply() {
	for msg := range kv.applyCh {
		if kv.killed() == true {
			return
		}

		DPrintf("[%d] receive apply msg: %v\n", kv.me, msg)
		if msg.SnapshotValid {
			kv.startIndex = msg.SnapshotIndex
			continue
		}

		kv.mu.Lock()
		// if need make snapshot
		if kv.maxraftstate != -1 && kv.lastApplied-kv.startIndex >= kv.maxraftstate {
			writer := new(bytes.Buffer)
			_ = labgob.NewEncoder(writer).Encode(kv.table)
			kv.rf.Snapshot(kv.lastApplied, writer.Bytes())
		}

		op := msg.Command.(Op)
		// only update when seq != current seq
		if op.Type != "Get" && kv.duplicateTable[op.ClientId] != op.Seq {
			kv.updateTable(op)
			kv.duplicateTable[op.ClientId] = op.Seq
		}

		kv.lastApplied += 1
		kv.lastAppliedCond.Broadcast()
		kv.mu.Unlock()
	}
}

func (kv *KVServer) updateTable(op Op) {
	if op.Type == "Put" {
		kv.table[op.Key] = op.Value
	} else {
		kv.table[op.Key] += op.Value
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.startIndex = 0
	kv.lastApplied = 0
	kv.lastAppliedCond = sync.NewCond(&kv.mu)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.table = map[string]string{}
	kv.duplicateTable = map[int64]int64{}

	go kv.apply()

	return kv
}
