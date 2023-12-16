package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"
import "6.5840/shardctrler"
import "time"

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	clientId int64
	seq      int64
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	ck.clientId = nrand()
	ck.seq = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	ck.seq += 1
	args := GetArgs{
		BaseArgs: BaseArgs{
			ClientId: ck.clientId,
			Seq:      ck.seq,
		},
		Key: key,
	}
	reply := ck.call(key2shard(key), "ShardKV.Get", "Get", &args)
	return reply.(*GetReply).Value
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.seq += 1
	args := PutAppendArgs{
		BaseArgs: BaseArgs{
			ClientId: ck.clientId,
			Seq:      ck.seq,
		},
		Key:   key,
		Value: value,
		Op:    op,
	}
	ck.call(key2shard(key), "ShardKV.PutAppend", "PutAppend", &args)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) call(shard int, method, typeName string, args Args) Reply {
	for {
		// ask controller for the latest configuration.
		ck.config = ck.sm.Query(-1)
		DPrintf("call, new config: %v\n", ck.config)

		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				reply := NewReply(typeName)
				DPrintf("[%d]:[%d] call %s[%d]:[%d] start, args = %s\n", ck.clientId, ck.seq, method, gid, si, args)
				ok := srv.Call(method, args, reply)
				DPrintf("[%d]:[%d] call %s[%d]:[%d] finished, ok = %v, reply = %s\n", ck.clientId, ck.seq, method, gid, si, ok, reply)
				if ok && reply.getErr() == OK || reply.getErr() == ErrNoKey {
					return reply
				}
				if ok && reply.getErr() == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
