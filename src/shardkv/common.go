package shardkv

import (
	"fmt"
	"log"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	OK                = "OK"
	ErrNoKey          = "ErrNoKey"
	ErrWrongGroup     = "ErrWrongGroup"
	ErrWrongLeader    = "ErrWrongLeader"
	ErrWrongTerm      = "ErrWrongTerm"
	ErrConfigNotMatch = "ErrConfigNotMatch"
)

type Err string

type Args interface{}

type BaseArgs struct {
	ClientId int64
	Seq      int64
}

type Reply interface {
	getErr() Err
}

type BaseReply struct {
	Err      Err
	ClientId int64
	Seq      int64
}

func (b *BaseReply) getErr() Err {
	return b.Err
}

// Put or Append
type PutAppendArgs struct {
	BaseArgs
	Key   string
	Value string
	Op    string // "Put" or "Append"
}

func (args *PutAppendArgs) String() string {
	return fmt.Sprintf("{Key = %v, Value = %v, Op = %v, ClientId = %d, Seq = %d}", args.Key, args.Value, args.Op, args.ClientId, args.Seq)
}

type PutAppendReply struct {
	BaseReply
}

func (reply *PutAppendReply) String() string {
	return fmt.Sprintf("{Err = %v}", reply.Err)
}

type GetArgs struct {
	BaseArgs
	Key string
}

func (args *GetArgs) String() string {
	return fmt.Sprintf("{Key = %v, ClientId = %d, Seq = %d}", args.Key, args.ClientId, args.Seq)
}

type GetReply struct {
	BaseReply
	Value string
}

func (reply *GetReply) String() string {
	return fmt.Sprintf("{Err = %v, Value = %v}", reply.Err, reply.Value)
}

type TransferArgs struct {
	ConfigNum  int
	Shard      int
	Table      map[string]string
	LastResult map[int64]Result
}

func (args *TransferArgs) String() string {
	return fmt.Sprintf("{ConfigNum = %v, Shard = %v, Table = %v, LastResult = %v}", args.ConfigNum, args.Shard, args.Table, args.LastResult)
}

type TransferReply struct {
	Err Err
}

var _ Args = (*GetArgs)(nil)
var _ Args = (*PutAppendArgs)(nil)
var _ Reply = (*GetReply)(nil)
var _ Reply = (*PutAppendReply)(nil)

func NewReply(typeName string) Reply {
	if typeName == "Get" {
		return &GetReply{}
	} else {
		return &PutAppendReply{}
	}
}
