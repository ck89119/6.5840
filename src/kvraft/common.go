package kvraft

import "fmt"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongTerm   = "ErrWrongTerm"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	ClientId int64
	Seq      int64
}

func (args *PutAppendArgs) String() string {
	return fmt.Sprintf("{Key = %v, Value = %v, Op = %v, ClientId = %d, Seq = %d}", args.Key, args.Value,
		args.Op, args.ClientId, args.Seq)
}

type PutAppendReply struct {
	Err Err
}

func (reply *PutAppendReply) String() string {
	return fmt.Sprintf("{Err = %v}", reply.Err)
}

type GetArgs struct {
	Key string
}

func (args *GetArgs) String() string {
	return fmt.Sprintf("{Key = %v}", args.Key)
}

type GetReply struct {
	Err   Err
	Value string
}

func (reply *GetReply) String() string {
	return fmt.Sprintf("{Err = %v, Value = %v}", reply.Err, reply.Value)
}
