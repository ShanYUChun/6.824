package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader int
	me         int64
	lastUid    int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.lastLeader = 0
	ck.me = nrand()
	ck.lastUid = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{
		Key:     key,
		UniId:   nrand(),
		CId:     ck.me,
		LastUid: ck.lastUid,
	}
	for {
		for i := 0; i < len(ck.servers); i++ {
			// reply 只能使用一次
			reply := GetReply{}
			if ok := ck.servers[(i+ck.lastLeader)%len(ck.servers)].Call("KVServer.Get", &args, &reply); ok {
				if reply.Err == ErrNoKey {
					ck.lastLeader = (i + ck.lastLeader) % len(ck.servers)
					ck.lastUid = args.UniId
					return ""
				} else if reply.Err == OK {
					ck.lastLeader = (i + ck.lastLeader) % len(ck.servers)
					ck.lastUid = args.UniId
					return reply.Value
				}
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:     key,
		Value:   value,
		Op:      op,
		UniId:   nrand(),
		CId:     ck.me,
		LastUid: ck.lastUid,
	}
	for {
		for i := 0; i < len(ck.servers); i++ {
			reply := PutAppendReply{}
			if ok := ck.servers[(i+ck.lastLeader)%len(ck.servers)].Call("KVServer.PutAppend", &args, &reply); ok {
				if reply.Err == OK {
					ck.lastLeader = (i + ck.lastLeader) % len(ck.servers)
					ck.lastUid = args.UniId
					return
				}
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
