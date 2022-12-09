package shardkv

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type         string // From client or kvserver
	Ope          string
	Key          string
	Value        string
	UniId        int64
	LastUid      int64
	Shard        int // operation on which shard
	ConfigNum    int
	ConfigShards [shardctrler.NShards]int
	Shardtable   map[string]string // whole shard that send from pre-server
	Unitset      map[int64]void    // Uniset for the shard
}

type ShardKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	tables    map[int]map[string]string // one shardkv will serve for several shard
	noti      map[int64](chan bool)
	debug     string
	uniSet    map[int]map[int64]void // convinient for server to pass uniset for one shard to another server
	persister *raft.Persister

	// for sending shard quickly
	// rpc cost time we cannot block the server for such a long time
	// the shards will still stored in this server until it send succ
	// but it cannot serve for client
	shardsState   [shardctrler.NShards]string // shardsstate to represent the state of shard in the aspect of this server
	curConfig     shardctrler.Config          // kvserver need to persisit confignum due to recover from snapshot, it cannot miss any config
	nextConfigNum int
	confirmList   map[int]int // shard -> confignum // the shard only confirm for new confirm, late confirm will cause shard disappear

	ctrClerk *shardctrler.Clerk

	rfIndex int // save the last index hand by rf
}

func (sk *ShardKV) task(op Op, uid int64) bool {

	_, _, isleader := sk.rf.Start(op)
	if isleader == false {
		return false
	}
	sk.mu.Lock()
	sk.noti[uid] = make(chan bool)
	ch := sk.noti[uid]
	sk.mu.Unlock()

	done := false

	select {
	case <-time.After(500 * time.Millisecond):
		fakech := make(chan void)
		go func() {
			for {
				select {
				case <-ch:
				case <-fakech:
					close(fakech)
					return
				}
			}
		}()
		sk.mu.Lock()
		fakech <- void{}
	case done = <-ch:
		fakech := make(chan void)
		go func() {
			for {
				select {
				case <-ch:
				case <-fakech:
					close(fakech)
					return
				}
			}
		}()
		sk.mu.Lock()
		fakech <- void{}
		// done = true
	}
	close(sk.noti[uid])
	delete(sk.noti, uid)
	sk.mu.Unlock()
	return done
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	// logic in get might be different in Get and Put

	kv.mu.Lock()
	if kv.shardsState[args.Shard] != SKeep {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	op := Op{
		Type:    "Outer",
		Ope:     "Get",
		Key:     args.Key,
		UniId:   args.UId,
		LastUid: args.LastUId,
		Shard:   args.Shard,
	}
	if ok := kv.task(op, args.UId); ok {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if kv.shardsState[args.Shard] != SKeep {
			reply.Err = ErrWrongGroup
		} else if value, ok := kv.tables[args.Shard][args.Key]; ok {
			reply.Err = OK
			reply.Value = value
		} else {
			reply.Err = ErrNoKey
		}
	} else {
		reply.Err = ErrWrongLeader
	}

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if kv.shardsState[args.Shard] != SKeep {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	op := Op{
		Type:    "Outer",
		Ope:     args.Op,
		Key:     args.Key,
		Value:   args.Value,
		UniId:   args.UId,
		LastUid: args.LastUId,
		Shard:   args.Shard,
	}
	if ok := kv.task(op, args.UId); ok {
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) applyChReciver() {
	var snapshotindex = 0
	for kv.killed() == false {
		msg := <-kv.applyCh
		DPrintf("%v got msg index %v from applCh\n", kv.debug, msg.CommandIndex)
		if msg.CommandValid {
			op := (msg.Command).(Op)
			kv.mu.Lock()
			DPrintf("%v applyCh got lock : opetration :%v\n", kv.debug, op.Ope)
			kv.rfIndex = msg.CommandIndex
			// this happen when there is reconfigration during rf commit
			// just send false back and server will return ErrWrongLeader
			// and client ask another server might in this group might not
			// and cm will tell client the latest config

			// send msg about can op canbe done
			notiMsg := true
			if op.Type == "Inner" {
				if op.Ope == "SendShard" {
					// think about if this shard server responible for
				}
				if op.Ope == "RequestShard" {

					// reject some request that may have already done
					if (kv.shardsState[op.Shard] != SRequesting && kv.shardsState[op.Shard] != SkeepAndRequesting) || kv.curConfig.Num > op.ConfigNum {
						notiMsg = false
					}
				}
				if op.Ope == "InstallShard" {
					if kv.curConfig.Num == op.ConfigNum && (kv.shardsState[op.Shard] == SRequesting || kv.shardsState[op.Shard] == SRequest || kv.shardsState[op.Shard] == SkeepAndRequest || kv.shardsState[op.Shard] == SkeepAndRequesting) {
						shardtable := make(map[string]string)
						uniset := make(map[int64]void)
						for key := range op.Shardtable {
							shardtable[key] = op.Shardtable[key]
						}
						for key := range op.Unitset {
							uniset[key] = op.Unitset[key]
						}
						kv.tables[op.Shard] = shardtable
						kv.uniSet[op.Shard] = uniset
						kv.shardsState[op.Shard] = SKeep
						// if everyshard just installed
						// and killed - restart
						// nextconfignum will got forget
						kv.nextConfigNum = kv.curConfig.Num + 1
						for _, state := range kv.shardsState {
							if state == SRequesting || state == SRequest || state == SkeepAndRequest || state == SkeepAndRequesting {
								kv.nextConfigNum = kv.curConfig.Num
							}
						}
						// once installed new shard
						// should persist snapshot
						// snapshotindex = msg.CommandIndex
						// kv.sendSnapshot()
					} else {
						notiMsg = false
					}
				}
				if op.Ope == "ConfirmShard" {
					// consider if some server confirm to late
					// and delete the shard that need by others
					if kv.confirmList[op.Shard] == op.ConfigNum {
						if kv.shardsState[op.Shard] == SKeepAndSendout {
							delete(kv.tables, op.Shard)
							kv.shardsState[op.Shard] = SNoKeep
						} else if kv.shardsState[op.Shard] == SkeepAndRequest || kv.shardsState[op.Shard] == SkeepAndRequesting {
							delete(kv.tables, op.Shard)
							kv.shardsState[op.Shard] = SRequest
						}
					} else {
						DPrintf("%v late confirm shard : %v", kv.debug, op.Shard)
					}
				}
				if op.Ope == "ConfigUpdate" {
					if kv.curConfig.Num > op.ConfigNum {
						notiMsg = false
					} else if kv.curConfig.Num < op.ConfigNum {
						kv.curConfig.Num = op.ConfigNum
						kv.curConfig.Shards = op.ConfigShards
						noupdate := true
						for i, gid := range op.ConfigShards {
							if gid == kv.gid && kv.shardsState[i] != SKeep {
								if kv.shardsState[i] == SKeepAndSendout {
									// might the last server do not comfirm his request
									// this might cause consistenly wait for commit and never request
									kv.shardsState[i] = SkeepAndRequest
								} else {
									kv.shardsState[i] = SRequest
								}
								noupdate = false
							} else if gid != kv.gid {
								if kv.shardsState[i] == SKeep {
									kv.shardsState[i] = SKeepAndSendout
									kv.confirmList[i] = kv.curConfig.Num
								} else if kv.shardsState[i] == SRequest || kv.shardsState[i] == SRequesting {
									kv.shardsState[i] = SNoKeep
									delete(kv.tables, i)
								} else if kv.shardsState[i] == SkeepAndRequest || kv.shardsState[i] == SkeepAndRequesting {
									kv.shardsState[i] = SNoKeep
									delete(kv.tables, i)
								}
							}
						}
						if noupdate {
							// if no update for server
							// just jump to next config
							kv.nextConfigNum = kv.nextConfigNum + 1
						}
					} else {
						// double check
						// not sure why install shard not work here
						// to make sure nextconfigNum will change when curconfig done
						kv.nextConfigNum = kv.curConfig.Num + 1
						for _, state := range kv.shardsState {
							if state == SRequesting || state == SRequest || state == SkeepAndRequest || state == SkeepAndRequesting {
								kv.nextConfigNum = kv.curConfig.Num
							}
						}
					}
				}
			} else if op.Type == "Outer" {
				if kv.shardsState[op.Shard] != SKeep {
					notiMsg = false
				} else {
					if _, ok := kv.uniSet[op.Shard][op.UniId]; ok == false {
						if op.Ope == "Put" {
							kv.tables[op.Shard][op.Key] = op.Value
							DPrintf("%v set key :%v to %v in %v", kv.debug, op.Key, op.Value, op.Shard)
						} else if op.Ope == "Append" {
							if _, ok := kv.tables[op.Shard][op.Key]; ok {
								kv.tables[op.Shard][op.Key] = kv.tables[op.Shard][op.Key] + op.Value
							} else {
								kv.tables[op.Shard][op.Key] = op.Value
							}
							DPrintf("%v set key :%v to %v in %v", kv.debug, op.Key, kv.tables[op.Shard][op.Key], op.Shard)
						}
						kv.uniSet[op.Shard][op.UniId] = void{}
						delete(kv.uniSet[op.Shard], op.LastUid)
					}
				}
			}
			// only when commited index update
			// will snapshot help rf to reduce its log
			DPrintf("%v rf state size%v", kv.debug, kv.persister.RaftStateSize())
			if kv.maxraftstate > 0 && kv.persister.RaftStateSize() > kv.maxraftstate && (kv.rfIndex > snapshotindex+10 || kv.persister.RaftStateSize() > 2*kv.maxraftstate) {
				// DPrintf("%v sendSnapshot to rf with index %v", kv.debug, kv.rfIndex)
				snapshotindex = msg.CommandIndex
				kv.sendSnapshot()
			}
			if _, ok := kv.noti[op.UniId]; ok {
				// DPrintf("%v apply ok,uuid%v\n", kv.debug, op.UniId)
				kv.noti[op.UniId] <- notiMsg
				// one posible thing
				// the later msg send from rf just before notification got the lock
				// and applych got lock again and issue another notification
				// the later notification got the lock first
			}
			DPrintf("%v applyCh unlock \n", kv.debug)
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			// DPrintf("%v recived snapshot from rf install", kv.debug)
			kv.mu.Lock()
			DPrintf("%v recived snapshot from rf install", kv.debug)
			kv.readSnapshot(msg.Snapshot)
			kv.mu.Unlock()
		}
	}
}

// this func will keep trying to request shard from others
// why not chose to send old shard to others becasue
// only the leader should accpet new shard
func (kv *ShardKV) SendShard(args *RequestShardArgs, reply *RequestShardReply) {
	DPrintf("%v Got sendding shard :%v", kv.debug, args.ShardNum)
	kv.mu.Lock()
	// if _, ok := kv.tables[args.ShardNum]; ok == false {
	// 	kv.mu.Unlock()
	// 	DPrintf("%v No shard :%v", kv.debug, args.ShardNum)
	// 	reply.Err = ErrWrongGroup
	// 	return
	// }
	// kv.shardsState[]
	// the quester might be advance to the sender
	// wait for config update or the config update will never success due to oldconfig check
	if args.ConfigNum > kv.curConfig.Num {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()
	op := Op{
		Type:  "Inner",
		Ope:   "SendShard",
		Shard: args.ShardNum,
		UniId: nrand(),
	}
	if ok := kv.task(op, op.UniId); ok {
		DPrintf("%v Sendding shard :%v", kv.debug, args.ShardNum)
		kv.mu.Lock()
		defer kv.mu.Unlock()
		reply.Err = OK
		// even validate the table before, but might not exist now
		// reject it if not exit
		// if _, ok := kv.tables[args.ShardNum]; ok == false {
		// 	reply.Err = ErrWrongGroup
		// 	return
		// }
		if kv.shardsState[args.ShardNum] != SKeepAndSendout && kv.shardsState[args.ShardNum] != SkeepAndRequest && kv.shardsState[args.ShardNum] != SkeepAndRequesting {
			reply.Err = ErrWrongGroup
			return
		}
		// the quester might be advance to the sender
		// when sender got sendingrequest
		// makesure the server wont server for shard anymore
		// delete(kv.shards, args.ShardNum)
		shardtable := make(map[string]string)
		uniset := make(map[int64]void)
		for key := range kv.tables[args.ShardNum] {
			shardtable[key] = kv.tables[args.ShardNum][key]
		}
		for key := range kv.uniSet[args.ShardNum] {
			uniset[key] = kv.uniSet[args.ShardNum][key]
		}
		reply.Shard = shardtable
		reply.Uniset = uniset
	} else {
		reply.Err = ErrWrongLeader
	}
}

// shrad have been confirmed by server in newconfig
// delete this shard
func (kv *ShardKV) Confirm(args *ConfirmArgs, reply *ConfirmReply) {
	DPrintf("%v Confirm for shard :%v", kv.debug, args.ShardNum)
	op := Op{
		Type:      "Inner",
		Ope:       "ConfirmShard",
		Shard:     args.ShardNum,
		ConfigNum: args.ConfigNum,
		UniId:     nrand(),
	}
	if ok := kv.task(op, op.UniId); ok {
		DPrintf("%v Confirm for shard :%v success", kv.debug, args.ShardNum)
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}
}

// no worry about what if the leader issused shard opretation then got killed or partitioned
// the next leader will takeover his job
func (kv *ShardKV) requestShard(shardnum int, confignum int, newconfig, oldconfig shardctrler.Config) {
	// should not check wether its the latest config
	// ervery config need to apply
	// newconfig := kv.ctrClerk.Query(confignum)
	// passing the config
	// check the config its too slow
	// if newconfig.Num != confignum {
	// 	return
	// }
	kv.mu.Lock()
	// if confignum != kv.curConfig.Num {
	// 	kv.shardsState[shardnum] =
	// 	kv.mu.Unlock()
	// 	return
	// }
	if kv.shardsState[shardnum] != SRequesting && kv.shardsState[shardnum] != SkeepAndRequesting {
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// DPrintf("%v Request for shard :%v in config :%v", kv.debug, shardnum, confignum)
	// op := Op{
	// 	Type:      "Inner",
	// 	Ope:       "RequestShard",
	// 	Shard:     shardnum,
	// 	ConfigNum: confignum,
	// 	UniId:     nrand(),
	// }
	// if ok := kv.task(op, op.UniId); ok == false {
	// 	// wg.Done()
	// 	// keep clear if one shard is required in new config
	// 	// requestshard return in two way
	// 	// fail : from requesting -> request
	// 	// suceess : install
	// 	// not sure why if in requesting and applych will reject
	// 	kv.mu.Lock()
	// 	if kv.shardsState[shardnum] == SRequesting {
	// 		kv.shardsState[shardnum] = SRequest
	// 	} else if kv.shardsState[shardnum] == SkeepAndRequesting {
	// 		kv.shardsState[shardnum] = SkeepAndRequest
	// 	}
	// 	kv.mu.Unlock()
	// 	return
	// }
	// kv.mu.Lock()
	// if kv.shardsState[shardnum] != SRequesting && kv.shardsState[shardnum] != SkeepAndRequesting {
	// 	kv.mu.Unlock()
	// 	return
	// }
	// kv.mu.Unlock()
	DPrintf("%v Request for shard :%v in config :%v", kv.debug, shardnum, confignum)
	args := RequestShardArgs{
		ShardNum:  shardnum,
		ConfigNum: confignum,
	}
	comfirmArg := ConfirmArgs{
		ShardNum:  shardnum,
		ConfigNum: confignum,
	}
	if confignum == 0 {
		// wg.Done()
		return
	}
	// oldconfig := kv.ctrClerk.Query(confignum - 1)
	gid := oldconfig.Shards[shardnum]
	if _, ok := oldconfig.Groups[gid]; ok == false {
		op := Op{
			Type:      "Inner",
			Ope:       "InstallShard",
			Shard:     shardnum,
			ConfigNum: confignum,
			UniId:     nrand(),
		}
		DPrintf("%v InstallShard:%v issued to rf", kv.debug, shardnum)
		// if install success, applych will change the state
		// if not just goes back to request state
		if ok2 := kv.task(op, op.UniId); ok2 == false {
			kv.mu.Lock()
			if kv.shardsState[shardnum] == SRequesting {
				kv.shardsState[shardnum] = SRequest
			} else if kv.shardsState[shardnum] == SkeepAndRequesting {
				kv.shardsState[shardnum] = SkeepAndRequest
			}
			kv.mu.Unlock()
		}
		// wg.Done()
		return
	}
	// defer wg.Done()
	for kv.killed() == false {
		// there always ask for old config about groupserver
		// when group leave there is no information about the group in newconfig
		DPrintf("%v Request for shard :%v in config :%v", kv.debug, shardnum, confignum)
		for _, servername := range oldconfig.Groups[gid] {
			server := kv.make_end(servername)
			reply := RequestShardReply{}
			// call -> package.function
			ok := server.Call("ShardKV.SendShard", &args, &reply)
			if ok && reply.Err == OK {
				DPrintf("%v Got shard :%v from server in %v", kv.debug, shardnum, gid)
				op := Op{
					Type:       "Inner",
					Ope:        "InstallShard",
					Shard:      shardnum,
					Shardtable: reply.Shard,
					ConfigNum:  confignum,
					Unitset:    reply.Uniset,
					UniId:      nrand(),
				}
				// if installed the shard, then confirm,
				// every operation will send to raft
				// so it's ok to delete the shard when successfully send to peers
				// raft log will help to rebuild the system
				if ok := kv.task(op, op.UniId); ok {
					DPrintf("%v Install shard :%v Success", kv.debug, shardnum)
					for kv.killed() == false {
						for _, servername2 := range oldconfig.Groups[gid] {
							server2 := kv.make_end(servername2)
							comfirmReply := ConfirmReply{}
							if ok := server2.Call("ShardKV.Confirm", &comfirmArg, &comfirmReply); ok {
								if comfirmReply.Err == OK {
									DPrintf("%v Send confirm shard :%v Success", kv.debug, shardnum)
									return
								}
							}
						}
						time.Sleep(100 * time.Millisecond)
					}
				} else {
					kv.mu.Lock()
					if kv.shardsState[shardnum] != SRequesting && kv.shardsState[shardnum] != SkeepAndRequesting {
						DPrintf("%v Late Install for:%v", kv.debug, shardnum)
					} else {
						DPrintf("%v Install failed:%v", kv.debug, shardnum)
					}
					kv.mu.Unlock()
					return
				}
			} else if reply.Err == ErrWrongGroup {
				// is there any prob err will be wronggroup?
				kv.mu.Lock()
				if kv.shardsState[shardnum] == SRequesting {
					kv.shardsState[shardnum] = SRequest
				} else if kv.shardsState[shardnum] == SkeepAndRequesting {
					kv.shardsState[shardnum] = SkeepAndRequest
				}
				kv.mu.Unlock()
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// check if one shard should send to other group
// and delete the shard from shards and wait for others to send request for this shard
// check if one shard should request from other group
// and send request to them
// only when config change or leader change need resend request
// no to miss any config
// play the config one by one wait until the prev config finished

func (kv *ShardKV) configCheck() {
	// if kv.curConfig.Num > 0 {
	// 	oldconfig := kv.ctrClerk.Query(kv.curConfig.Num - 1)
	// 	for i, state := range kv.shardsState {
	// 		if state == SKeep {
	// 			go kv.sendComfirm(i)
	// 		}
	// 	}
	// }
	for kv.killed() == false {
		// query(-1) might miss config
		// and need to query again
		// just query the next config we want
		// if lastnum +1 not exits
		// will return the latest config
		// config := kv.ctrClerk.Query(lastNum + 1)
		// if config.Num > lastNum+1 {
		// 	config = kv.ctrClerk.Query(lastNum + 1)
		// }
		kv.mu.Lock()
		// lastNum should update
		// possible sisuation:
		// might server one config is finished and
		// DPrintf("%v ConfigCheck Lock", kv.debug)
		newconfig := false
		config := kv.curConfig
		if kv.nextConfigNum > kv.curConfig.Num {
			config = kv.ctrClerk.Query(kv.nextConfigNum)
			if config.Num < kv.nextConfigNum {
				kv.mu.Unlock()
				time.Sleep(100 * time.Millisecond)
				continue
			}
			newconfig = true

		}
		kv.mu.Unlock()
		op := Op{
			Type:         "Inner",
			Ope:          "ConfigUpdate",
			ConfigNum:    config.Num,
			UniId:        nrand(),
			ConfigShards: config.Shards,
		}
		DPrintf("%v  %v", kv.debug, kv.shardsState)
		if ok := kv.task(op, op.UniId); ok == false {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		DPrintf("%v  %v", kv.debug, kv.shardsState)
		DPrintf("%v ConfigCheck Lock", kv.debug)
		DPrintf("%v confignum %v", kv.debug, config.Num)
		// get state may will cause dead lock
		// rf msg channel protected by lock
		// get state need lock
		// even there is a leader in this term
		// for the begining of time
		// there is no leader in this term
		// at least there should be one leader then
		// the request could go on
		// term, isLeader := kv.rf.GetState()
		// DPrintf("%v confignum %v", kv.debug, config.Num)
		// if lastNum == config.Num && term == lastTerm {
		// 	DPrintf("%v nochange %v", kv.debug, config.Num)
		// 	time.Sleep(100 * time.Millisecond)
		// 	continue
		// }
		kv.mu.Lock()
		DPrintf("%v ConfigCheck Lock", kv.debug)
		// check the old config have done
		// oldconfig := kv.ctrClerk.Query(lastNum)
		// for i, gid := range curconfig.Shards {
		// 	if _, ok := kv.shards[i]; !ok && gid == kv.gid {
		// 		config = curconfig
		// 		break
		// 	}
		// }
		DPrintf("%vFind new config: %v(config change ?:%v)", kv.debug, config.Num, newconfig)
		// lastNum = config.Num
		// slave might never reach there
		// then it become leader
		// the old config might too old
		oldconfig := kv.ctrClerk.Query(config.Num - 1)
		// if isLeader == false {
		// 	kv.mu.Unlock()
		// 	time.Sleep(100 * time.Millisecond)
		// 	continue
		// }
		// lastTerm = term
		DPrintf("%v", config.Shards)
		DPrintf("%v", oldconfig.Shards)
		for i, state := range kv.shardsState {
			// check kv shard instead of kv table
			// kv table delete after comfirm
			// comfirm might later than new config happened
			if state == SRequest {
				kv.shardsState[i] = SRequesting
				go kv.requestShard(i, config.Num, config, oldconfig)
			} else if state == SkeepAndRequest {
				kv.shardsState[i] = SkeepAndRequesting
				go kv.requestShard(i, config.Num, config, oldconfig)
			}
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

// send snapshot to raft
func (kv *ShardKV) sendSnapshot() {
	// kv.mu.Lock()
	// defer kv.mu.Unlock()

	DPrintf("%v sendSnapshot to rf with index %v", kv.debug, kv.rfIndex)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.tables)
	e.Encode(kv.rfIndex)
	e.Encode(kv.uniSet)
	e.Encode(kv.shardsState)
	e.Encode(kv.curConfig)
	e.Encode(kv.confirmList)

	snapshot := w.Bytes()

	kv.rf.Snapshot(kv.rfIndex, snapshot)
}

// load snapshot after recovery
// or hand from rf due to stale
func (kv *ShardKV) readSnapshot(data []byte) {
	// DPrintf("%v readsnapshot", kv.debug)
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var tables map[int]map[string]string
	var index int
	var uniset map[int]map[int64]void
	var shardsState [shardctrler.NShards]string
	var curconfig shardctrler.Config
	var confirmlist map[int]int

	if d.Decode(&tables) != nil ||
		d.Decode(&index) != nil ||
		d.Decode(&uniset) != nil ||
		d.Decode(&shardsState) != nil ||
		d.Decode(&curconfig) != nil ||
		d.Decode(&confirmlist) != nil {
		// DPrintf("%v readsnapshot fail", kv.debug)
	} else {
		kv.tables = tables
		kv.rfIndex = index
		kv.uniSet = uniset
		kv.shardsState = shardsState
		kv.curConfig = curconfig
		kv.confirmList = confirmlist
	}
	DPrintf("%vreadsnapshot at index :%v", kv.debug, index)
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//

func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
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
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.ctrClerk = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.persister = persister

	kv.noti = make(map[int64]chan bool)
	kv.tables = make(map[int]map[string]string)
	kv.rfIndex = 0
	kv.uniSet = make(map[int]map[int64]void)
	kv.confirmList = make(map[int]int)
	for i := range kv.shardsState {
		kv.shardsState[i] = SNoKeep
	}
	kv.nextConfigNum = 1

	for i := 0; i < kv.gid-100; i++ {
		kv.debug += "                                                                                                                                         "
	}
	for i := 0; i < kv.me; i++ {
		kv.debug += "                               "
	}
	DPrintf("%vWait up", kv.debug)
	kv.readSnapshot(persister.ReadSnapshot())
	DPrintf("%vWait up", kv.debug)
	kv.nextConfigNum = kv.curConfig.Num + 1
	for i := range kv.shardsState {
		if kv.shardsState[i] == SRequesting || kv.shardsState[i] == SRequest {
			kv.shardsState[i] = SRequest
			kv.nextConfigNum = kv.curConfig.Num
		} else if kv.shardsState[i] == SkeepAndRequest || kv.shardsState[i] == SkeepAndRequesting {
			kv.shardsState[i] = SkeepAndRequest
			kv.nextConfigNum = kv.curConfig.Num
		}
	}
	go kv.configCheck()
	go kv.applyChReciver()

	return kv
}
