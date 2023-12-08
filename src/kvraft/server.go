package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	OpTypeGet    = "Get"
	OpTypePut    = "Put"
	OpTypeAppend = "Append"
)

type OpType string

type Result struct {
	Seq   int64
	Err   Err
	Value string
}

type Op struct {
	Type     OpType
	Key      string
	Value    string
	ClientId int64
	Seq      int64
}

func (o Op) String() string {
	return fmt.Sprintf("{Type = %v, Key = %v, Value = %v, ClientId = %v, Seq = %v}", o.Type, o.Key, o.Value, o.ClientId, o.Seq)
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	lastApplied     int
	lastAppliedCond *sync.Cond

	table      map[string]string
	lastResult map[int64]Result
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("[%d] handle Get start, args = %s\n", kv.me, args)

	kv.mu.Lock()
	lastResult := kv.lastResult[args.ClientId]
	kv.mu.Unlock()
	if lastResult.Seq == args.Seq {
		DPrintf("[%d] duplicate Get, args = %s\n", kv.me, args)
		reply.Err = lastResult.Err
		reply.Value = lastResult.Value
		return
	}

	err := kv.waitForApplied(&Op{
		Type:     OpTypeGet,
		Key:      args.Key,
		ClientId: args.ClientId,
		Seq:      args.Seq,
	})

	reply.Err = err
	if err == OK {
		kv.mu.Lock()
		reply.Value = kv.lastResult[args.ClientId].Value
		kv.mu.Unlock()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("[%d] handle PutAppend start, args = %s\n", kv.me, args)

	kv.mu.Lock()
	lastResult := kv.lastResult[args.ClientId]
	kv.mu.Unlock()
	if lastResult.Seq == args.Seq {
		DPrintf("[%d] duplicate PutAppend, args = %s\n", kv.me, args)
		reply.Err = OK
		return
	}

	err := kv.waitForApplied(&Op{
		Type:     OpType(args.Op),
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		Seq:      args.Seq,
	})

	reply.Err = err
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
			kv.installSnapshot(msg.Snapshot)
			continue
		}

		kv.mu.Lock()
		op := msg.Command.(Op)
		// only update when last seq != current seq
		if kv.lastResult[op.ClientId].Seq != op.Seq {
			// if get value in Get(), get mutex failed, and mutex gained by above Lock(line 168), table will be updated by next msg
			if op.Type == OpTypeGet {
				// must get result here
				if value, ok := kv.table[op.Key]; ok {
					kv.lastResult[op.ClientId] = Result{
						Seq:   op.Seq,
						Err:   OK,
						Value: value,
					}
				} else {
					kv.lastResult[op.ClientId] = Result{
						Seq:   op.Seq,
						Err:   ErrNoKey,
						Value: "",
					}
				}
			} else {
				kv.updateTable(&op)
				kv.lastResult[op.ClientId] = Result{
					Seq:   op.Seq,
					Err:   OK,
					Value: "",
				}
			}
		}

		kv.lastApplied = msg.CommandIndex
		kv.lastAppliedCond.Broadcast()
		kv.mu.Unlock()
	}
}

func (kv *KVServer) updateTable(op *Op) {
	if op.Type == OpTypePut {
		kv.table[op.Key] = op.Value
	} else {
		kv.table[op.Key] += op.Value
	}
}

func (kv *KVServer) snapshot() {
	if kv.maxraftstate == -1 {
		return
	}

	for kv.killed() == false {
		if kv.persister.RaftStateSize() >= kv.maxraftstate {
			kv.mu.Lock()
			lastApplied := kv.lastApplied
			writer := new(bytes.Buffer)
			encoder := labgob.NewEncoder(writer)
			_ = encoder.Encode(lastApplied)
			_ = encoder.Encode(kv.table)
			_ = encoder.Encode(kv.lastResult)
			DPrintf("[%d] snapshot() lastApplied = %v, len(table) = %v, len(lastResult) = %v\n", kv.me,
				kv.lastApplied, len(kv.table), len(kv.lastResult))
			kv.mu.Unlock()

			kv.rf.Snapshot(lastApplied, writer.Bytes())
			DPrintf("[%d] snapshot() complete", kv.me)
		}

		time.Sleep(20 * time.Millisecond)
	}
}

func (kv *KVServer) installSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		DPrintf("[%d] empty snapshot", kv.me)
		return
	}
	DPrintf("[%d] len(snapshot) = %d", kv.me, len(snapshot))

	reader := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(reader)
	var lastApplied int
	var table map[string]string
	var lastResult map[int64]Result
	_ = decoder.Decode(&lastApplied)
	_ = decoder.Decode(&table)
	_ = decoder.Decode(&lastResult)
	DPrintf("[%d] installSnapshot() lastApplied = %v, len(table) = %v, len(lastResult) = %v\n", kv.me,
		lastApplied, len(table), len(lastResult))

	kv.mu.Lock()
	kv.lastApplied = lastApplied
	kv.table = table
	kv.lastResult = lastResult
	kv.mu.Unlock()
}

func (kv *KVServer) waitForApplied(op *Op) Err {
	lastLogIndex, term, isLeader := kv.rf.Start(*op)
	DPrintf("[%d] lastLogIndex = %v, term = %v, isLeader = %v\n", kv.me, lastLogIndex, term, isLeader)
	if !isLeader {
		return ErrWrongLeader
	}

	kv.lastAppliedCond.L.Lock()
	defer kv.lastAppliedCond.L.Unlock()
	for kv.lastApplied < lastLogIndex {
		kv.lastAppliedCond.Wait()
	}

	appliedLogTerm := kv.rf.GetLogTerm(lastLogIndex)
	if appliedLogTerm != term {
		DPrintf("[%d] appliedLogTerm = %v, term = %v\n", kv.me, appliedLogTerm, term)
		return ErrWrongTerm
	}

	return OK
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
	kv.persister = persister
	kv.lastApplied = 0
	kv.lastAppliedCond = sync.NewCond(&kv.mu)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.table = map[string]string{}
	kv.lastResult = map[int64]Result{}

	kv.installSnapshot(persister.ReadSnapshot())

	go kv.apply()
	go kv.snapshot()

	return kv
}
