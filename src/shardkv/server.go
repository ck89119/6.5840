package shardkv

import (
	"6.5840/labrpc"
	"6.5840/shardctrler"
	"bytes"
	"fmt"
	"sync/atomic"
	"time"
)
import "6.5840/raft"
import "sync"
import "6.5840/labgob"

const (
	OpTypeGet               = "Get"
	OpTypePut               = "Put"
	OpTypeAppend            = "Append"
	OpTypeConfigUpdate      = "ConfigUpdate"
	OpTypeShardStatusChange = "ShardStatusChange"
)

type OpType string

type ApplyResult struct {
	Err   Err
	Value string
}

type Op struct {
	Type OpType

	// normal op
	Key      string
	Value    string
	ClientId int64
	Seq      int64

	// config update
	NewConfig shardctrler.Config

	// shard status change
	ConfigNum  int
	Shard      int
	NewStatus  ShardStatus
	Table      map[string]string
	LastResult map[int64]Result
}

func (op Op) String() string {
	if op.Type == OpTypeGet || op.Type == OpTypePut || op.Type == OpTypeAppend {
		return fmt.Sprintf("{Type = %v, Key = %v, Value = %v, ClientId = %v, Seq = %v}", op.Type, op.Key, op.Value, op.ClientId, op.Seq)
	} else if op.Type == OpTypeConfigUpdate {
		return fmt.Sprintf("{Type = %v, NewConfig = %v}", op.Type, op.NewConfig)
	} else {
		return fmt.Sprintf("{Type = %v, ConfigNum = %v, Shard = %v, NewStatus = %v, Table = %v, LastResult = %v}", op.Type, op.ConfigNum, op.Shard, op.NewStatus, op.Table, op.LastResult)
	}
}

const (
	ShardStatusOffline   = 0
	ShardStatusReceiving = 1
	ShardStatusOffering  = 2
	ShardStatusPushing   = 3
)

type ShardStatus int

type Result struct {
	Seq   int64
	Err   Err
	Value string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	dead         int32 // set by Kill()
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	// Your definitions here.
	mck *shardctrler.Clerk

	table       map[string]string
	lastResult  map[int64]Result
	shardStatus [shardctrler.NShards]ShardStatus
	curConfig   shardctrler.Config

	lastApplied     int
	lastAppliedCond *sync.Cond
	waitChan        map[int]chan Err
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	DPrintf("[%d]:[%d] handle Get start, args = %s\n", kv.gid, kv.me, args)

	kv.mu.Lock()
	lastResult := kv.lastResult[args.ClientId]
	kv.mu.Unlock()
	if lastResult.Seq == args.Seq {
		DPrintf("[%d]:[%d] duplicate Get, args = %s\n", kv.gid, kv.me, args)
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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("[%d]:[%d] handle PutAppend start, args = %s\n", kv.gid, kv.me, args)

	kv.mu.Lock()
	lastResult := kv.lastResult[args.ClientId]
	kv.mu.Unlock()
	if lastResult.Seq == args.Seq {
		DPrintf("[%d]:[%d] duplicate PutAppend, args = %s\n", kv.gid, kv.me, args)
		reply.Err = lastResult.Err
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

func (kv *ShardKV) Transfer(args *TransferArgs, reply *TransferReply) {
	DPrintf("[%d]:[%d] handle Transfer start, args = %s\n", kv.gid, kv.me, args)

	kv.mu.Lock()
	ch := make(chan Err)
	lastLogIndex, term, isLeader := kv.rf.Start(Op{
		Type:       OpTypeShardStatusChange,
		ConfigNum:  args.ConfigNum,
		Shard:      args.Shard,
		NewStatus:  ShardStatusOffering,
		Table:      args.Table,
		LastResult: args.LastResult,
	})
	DPrintf("[%d]:[%d] lastLogIndex = %v, term = %v, isLeader = %v\n", kv.gid, kv.me, lastLogIndex, term, isLeader)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	kv.waitChan[lastLogIndex] = ch
	kv.mu.Unlock()

	reply.Err = <-ch
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) waitForApplied(op *Op) Err {
	lastLogIndex, term, isLeader := kv.rf.Start(*op)
	DPrintf("[%d]:[%d] lastLogIndex = %v, term = %v, isLeader = %v\n", kv.gid, kv.me, lastLogIndex, term, isLeader)
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

	// if it's a normal op, return last result
	if op.Type != OpTypeShardStatusChange {
		// if no such seq record, must be ErrWrongGroup
		if result := kv.lastResult[op.ClientId]; result.Seq != op.Seq {
			return ErrWrongGroup
		} else {
			return result.Err
		}
	}

	// OpTypeShardStatusChange, return OK
	return OK
}

func (kv *ShardKV) handleConfigUpdate(op *Op) {
	// enter with kv.mu locked
	if kv.curConfig.Num+1 != op.NewConfig.Num {
		DPrintf("[%d]:[%d] can't apply new config, config num not matched, curConfig.Num = %v, newConfig.Num = %v\n",
			kv.gid, kv.me, kv.curConfig.Num, op.NewConfig.Num)
		return
	}

	for shard, status := range kv.shardStatus {
		if status == ShardStatusPushing || status == ShardStatusReceiving {
			DPrintf("[%d]:[%d] can't apply new config, shard[%d] is %v\n", kv.gid, kv.me, shard, status)
			return
		}
	}
	DPrintf("[%d]:[%d] find new config: %v\n", kv.gid, kv.me, op.NewConfig)

	// push
	for shard, status := range kv.shardStatus {
		newGid := op.NewConfig.Shards[shard]
		if status == ShardStatusOffering && newGid != kv.gid {
			kv.rf.Start(Op{
				Type:      OpTypeShardStatusChange,
				ConfigNum: op.NewConfig.Num,
				Shard:     shard,
				NewStatus: ShardStatusPushing,
			})
			DPrintf("[%d]:[%d] pushing shard[%d]\n", kv.gid, kv.me, shard)
		}
	}

	// receive
	for shard, status := range kv.shardStatus {
		newGid := op.NewConfig.Shards[shard]
		if status == ShardStatusOffline && newGid == kv.gid {
			kv.rf.Start(Op{
				Type:      OpTypeShardStatusChange,
				ConfigNum: op.NewConfig.Num,
				Shard:     shard,
				NewStatus: ShardStatusReceiving,
			})
			DPrintf("[%d]:[%d] receiving shard[%d]\n", kv.gid, kv.me, shard)
		}
	}

	kv.curConfig = op.NewConfig
}

func (kv *ShardKV) handleShardStatusChange(op *Op, ch chan Err) {
	// enter with kv.mu locked

	if kv.curConfig.Num < op.ConfigNum {
		DPrintf("[%d]:[%d] NewConfig is too new, curConfig.Num = %v, NewConfig.Num = %v\n", kv.gid, kv.me, kv.curConfig.Num, op.ConfigNum)
		if ch != nil {
			ch <- ErrConfigNotMatch
		}
		return
	}

	if kv.curConfig.Num > op.ConfigNum {
		DPrintf("[%d]:[%d] NewConfig is too old, ignore, curConfig.Num = %v, NewConfig.Num = %v\n", kv.gid, kv.me, kv.curConfig.Num, op.ConfigNum)
		if ch != nil {
			ch <- OK
		}
		return
	}

	shard := op.Shard
	if kv.shardStatus[shard] == op.NewStatus {
		DPrintf("[%d]:[%d] shard[%d], curStatus(%v) == targetStatus(%v)\n", kv.gid, kv.me, shard, kv.shardStatus[shard], op.NewStatus)
		if ch != nil {
			ch <- OK
		}
		return
	}

	if (kv.shardStatus[shard]+1)%4 != op.NewStatus {
		DPrintf("[%d]:[%d] shard[%d] invalid status change, %v -> %v\n", kv.gid, kv.me, shard, kv.shardStatus[shard], op.NewStatus)
		if ch != nil {
			ch <- ErrConfigNotMatch
		}
		return
	}

	if op.NewStatus == ShardStatusPushing {
		go kv.transfer(shard)
	} else if op.NewStatus == ShardStatusOffering {
		for key, val := range op.Table {
			kv.table[key] = val
		}
		for key, val := range op.LastResult {
			if val.Seq > kv.lastResult[key].Seq {
				kv.lastResult[key] = val
			}
		}
	} else if op.NewStatus == ShardStatusOffline {
		for key := range kv.table {
			if key2shard(key) == shard {
				delete(kv.table, key)
			}
		}
	} else if op.NewStatus == ShardStatusReceiving {
		if kv.curConfig.Num == 1 {
			op.NewStatus = ShardStatusOffering
		}
	}

	kv.shardStatus[shard] = op.NewStatus
	DPrintf("[%d]:[%d] shard[%d] start new state: %v\n", kv.gid, kv.me, shard, op.NewStatus)
	if ch != nil {
		ch <- OK
	}
}

func (kv *ShardKV) handleOp(op *Op) {
	// enter with kv.mu locked

	// check key group first
	shard := key2shard(op.Key)
	if kv.shardStatus[shard] != ShardStatusOffering {
		DPrintf("[%d]:[%d] ErrWrongGroup, shard = %v, shardStatus = %v\n", kv.gid, kv.me, shard, kv.shardStatus)
		return
	}

	// duplicate request
	if kv.lastResult[op.ClientId].Seq == op.Seq {
		DPrintf("[%d]:[%d] duplicate request, op.ClientId = %v, op.Seq = %v\n", kv.gid, kv.me, op.ClientId, op.Seq)
		return
	}

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
		kv.updateTable(op)
		kv.lastResult[op.ClientId] = Result{
			Seq:   op.Seq,
			Err:   OK,
			Value: "",
		}
	}
}

func (kv *ShardKV) updateTable(op *Op) {
	if op.Type == OpTypePut {
		kv.table[op.Key] = op.Value
	} else {
		kv.table[op.Key] += op.Value
	}
}

func (kv *ShardKV) installSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		DPrintf("[%d]:[%d] empty snapshot", kv.gid, kv.me)
		return
	}
	DPrintf("[%d]:[%d] len(snapshot) = %d", kv.gid, kv.me, len(snapshot))

	reader := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(reader)
	var lastApplied int
	var table map[string]string
	var lastResult map[int64]Result
	var shardStatus [shardctrler.NShards]ShardStatus
	var curConfig shardctrler.Config
	_ = decoder.Decode(&lastApplied)
	_ = decoder.Decode(&table)
	_ = decoder.Decode(&lastResult)
	_ = decoder.Decode(&shardStatus)
	_ = decoder.Decode(&curConfig)
	DPrintf("[%d]:[%d] installSnapshot() lastApplied = %v, table = %v, lastResult = %v, shardStatus = %v, curConfig = %v\n",
		kv.gid, kv.me, lastApplied, table, lastResult, shardStatus, curConfig)

	kv.mu.Lock()
	kv.lastApplied = lastApplied
	kv.table = table
	kv.lastResult = lastResult
	kv.shardStatus = shardStatus
	kv.curConfig = curConfig

	for shard, status := range kv.shardStatus {
		if status == ShardStatusPushing {
			go kv.transfer(shard)
		}
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) apply() {
	for msg := range kv.applyCh {
		DPrintf("[%d]:[%d] receive apply msg: %v\n", kv.gid, kv.me, msg)

		if msg.SnapshotValid {
			kv.installSnapshot(msg.Snapshot)
			continue
		}

		kv.mu.Lock()
		op := msg.Command.(Op)
		if op.Type == OpTypeConfigUpdate {
			kv.handleConfigUpdate(&op)
		} else if op.Type == OpTypeShardStatusChange {
			ch := kv.waitChan[msg.CommandIndex]
			kv.handleShardStatusChange(&op, ch)
		} else {
			kv.handleOp(&op)
		}
		kv.lastApplied = msg.CommandIndex
		kv.lastAppliedCond.Broadcast()
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) snapshotTicker() {
	if kv.maxraftstate == -1 {
		return
	}

	for !kv.killed() {
		if kv.persister.RaftStateSize() >= kv.maxraftstate {
			kv.mu.Lock()
			lastApplied := kv.lastApplied
			writer := new(bytes.Buffer)
			encoder := labgob.NewEncoder(writer)
			_ = encoder.Encode(lastApplied)
			_ = encoder.Encode(kv.table)
			_ = encoder.Encode(kv.lastResult)
			_ = encoder.Encode(kv.shardStatus)
			_ = encoder.Encode(kv.curConfig)
			DPrintf("[%d]:[%d] snapshot() lastApplied = %v, table = %v, lastResult = %v, shardStatus = %v, curConfig = %v\n",
				kv.gid, kv.me, kv.lastApplied, kv.table, kv.lastResult, kv.shardStatus, kv.curConfig)
			kv.mu.Unlock()

			kv.rf.Snapshot(lastApplied, writer.Bytes())
			DPrintf("[%d]:[%d] snapshot() complete", kv.gid, kv.me)
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func (kv *ShardKV) fetchConfigurationTicker() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			configNum := kv.curConfig.Num
			kv.mu.Unlock()
			newConfig := kv.mck.Query(configNum + 1)
			kv.rf.Start(Op{
				Type:      OpTypeConfigUpdate,
				NewConfig: newConfig,
			})
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) transfer(shard int) {
	kv.mu.Lock()
	gid := kv.curConfig.Shards[shard]
	servers := kv.curConfig.Groups[gid]
	status := kv.shardStatus[shard]
	configNum := kv.curConfig.Num

	args := TransferArgs{
		ConfigNum:  configNum,
		Shard:      shard,
		Table:      map[string]string{},
		LastResult: map[int64]Result{},
	}
	for key, val := range kv.table {
		if key2shard(key) == shard {
			args.Table[key] = val
		}
	}
	for key, val := range kv.lastResult {
		args.LastResult[key] = val
	}
	kv.mu.Unlock()

	for status == ShardStatusPushing {
		if _, isLeader := kv.rf.GetState(); isLeader {
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				reply := TransferReply{}
				DPrintf("[%d]:[%d] call ShardKV.Transfer[%d]:[%d] start, args = %s\n", kv.gid, kv.me, gid, si, &args)
				ok := srv.Call("ShardKV.Transfer", &args, &reply)
				DPrintf("[%d]:[%d] call ShardKV.Transfer[%d]:[%d] finish, reply = %v\n", kv.gid, kv.me, gid, si, &reply)
				if ok && reply.Err == OK {
					lastLogIndex, term, isLeader := kv.rf.Start(Op{
						Type:      OpTypeShardStatusChange,
						ConfigNum: configNum,
						Shard:     shard,
						NewStatus: ShardStatusOffline,
					})
					DPrintf("[%d]:[%d] shard[%d] turn to offline, lastLogIndex = %v, term = %v, isLeader = %v\n", kv.gid, kv.me, shard, lastLogIndex, term, isLeader)
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)

		kv.mu.Lock()
		status = kv.shardStatus[shard]
		kv.mu.Unlock()
	}
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	DPrintf("[%d]:[%d] StartServer\n", gid, me)

	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(GetArgs{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(TransferArgs{})

	kv := new(ShardKV)
	kv.me = me
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.maxraftstate = maxraftstate
	kv.persister = persister
	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.table = map[string]string{}
	kv.lastResult = map[int64]Result{}

	kv.lastApplied = 0
	kv.lastAppliedCond = sync.NewCond(&kv.mu)
	kv.waitChan = map[int]chan Err{}

	kv.installSnapshot(persister.ReadSnapshot())

	go kv.apply()
	go kv.snapshotTicker()
	go kv.fetchConfigurationTicker()

	return kv
}
