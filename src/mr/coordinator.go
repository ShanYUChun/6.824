package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	MapTask           chan mapid
	ReduceTask        chan int
	nMapTask          int
	nReduceTask       int
	NReduce           int
	MapFinished       bool
	ReduceFinished    bool
	WaitMap           map[int]bool
	WaitReduce        map[int]bool
	mu_WaitMap        sync.Mutex
	mu_WaitReduce     sync.Mutex
	mu_nMapTask       sync.Mutex
	mu_nReduceTask    sync.Mutex
	mu_MapFinished    sync.RWMutex
	mu_ReduceFinished sync.RWMutex
}

type mapid struct {
	filename string
	idx      int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) SendTask(args *GetArgs, reply *GetReply) error {
	// ugly code
	reply.NReduce = c.NReduce
	c.mu_MapFinished.RLock()
	c.mu_ReduceFinished.RLock()
	if c.MapFinished && c.ReduceFinished {
		c.mu_MapFinished.RUnlock()
		c.mu_ReduceFinished.RUnlock()
		reply.Rtask = Task{"Finished", "", 0}
		return nil
	}
	c.mu_MapFinished.RUnlock()
	c.mu_ReduceFinished.RUnlock()
	select {
	case maptask := <-c.MapTask:
		{
			reply.Rtask = Task{"Map", maptask.filename, maptask.idx}
			// fmt.Printf("Map Task Assgined%v\n", maptask.idx)
			go c.timecounter("Map", maptask.filename, maptask.idx)
		}
	case reducetask := <-c.ReduceTask:
		{
			reply.Rtask = Task{"Reduce", "./", reducetask}
			// fmt.Printf("Reduce Task Assgined%v\n", reducetask)
			go c.timecounter("Reduce", "./", reducetask)
		}
	default:
		{
			reply.Rtask = Task{"Sleep", "", 0}
			// fmt.Printf("No Task\n")
		}
	}
	return nil
}

func (c *Coordinator) timecounter(task string, file string, id int) {
	time.Sleep(time.Duration(time.Second * 10))
	if task == "Map" {
		c.mu_WaitMap.Lock()
		if ok := c.WaitMap[id]; ok {
			c.mu_WaitMap.Unlock()
			// if push lock after c.MapTask --> deadlock
			c.MapTask <- mapid{file, id}
			// fmt.Printf("Map task %v unfinished over 10 seonds, task fail, assgin to other\n", id)
		} else {
			c.mu_WaitMap.Unlock()
		}
	} else {
		c.mu_WaitReduce.Lock()
		if ok := c.WaitReduce[id]; ok {
			c.mu_WaitReduce.Unlock()
			c.ReduceTask <- id
			// fmt.Printf("Reduce task %v unfinished over 10 seonds, task fail, assgin to other\n", id)
		} else {
			c.mu_WaitReduce.Unlock()
		}
	}
}

func (c *Coordinator) TaskDone(args *SendArgs, reply *SendReply) error {
	if args.Tname == "Map" {
		// this will cause race add mutex
		if ok := c.removeTask("Map", args.Tid); ok {
			reply.Message = "Accepted"
			// fmt.Printf("Map Task :%v finished\n", args.Tid)
		} else {
			reply.Message = "Reject"
			// fmt.Printf("Map Task :%v Reject\n", args.Tid)
		}
	} else {
		if ok := c.removeTask("Reduce", args.Tid); ok {
			reply.Message = "Accepted"
			// fmt.Printf("Reduce Task :%v finished\n", args.Tid)
		} else {
			reply.Message = "Reject"
			// fmt.Printf("Reduce Task :%v Reject\n", args.Tid)
		}
	}
	return nil
}

func (c *Coordinator) removeTask(tasktype string, taskid int) bool {
	if tasktype == "Map" {
		c.mu_WaitMap.Lock()
		if !c.WaitMap[taskid] {
			c.mu_WaitMap.Unlock()
			return false
		}
		c.WaitMap[taskid] = false
		c.mu_WaitMap.Unlock()
		c.mu_nMapTask.Lock()
		// 判断 Task Finished 必须在 Task 计数 后面执行，也必须执行 （或者放在每次 Done询问的时候判断）
		// 如果放在前面，就会出现 Task Finished 永远不会被改变，因为当 task 结束，永远不会进行下一次 finished 判断
		c.nMapTask = c.nMapTask - 1
		if c.nMapTask == 0 {
			c.mu_MapFinished.Lock()
			c.MapFinished = true
			c.mu_MapFinished.Unlock()
		}
		c.mu_nMapTask.Unlock()
	} else {
		c.mu_WaitReduce.Lock()
		if !c.WaitReduce[taskid] {
			c.mu_WaitReduce.Unlock()
			return false
		}
		c.WaitReduce[taskid] = false
		c.mu_WaitReduce.Unlock()
		c.mu_nReduceTask.Lock()
		c.nReduceTask = c.nReduceTask - 1
		if c.nReduceTask == 0 {
			c.mu_ReduceFinished.Lock()
			c.ReduceFinished = true
			c.mu_ReduceFinished.Unlock()
		}
		c.mu_nReduceTask.Unlock()
	}
	return true
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	// 这里因为 每次生成的sock 的名字一样，所以要移除之前的
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	// // fmt.Printf("Haven't Done\n")
	c.mu_MapFinished.RLock()
	c.mu_ReduceFinished.RLock()
	if c.MapFinished && c.ReduceFinished {
		c.mu_MapFinished.RUnlock()
		c.mu_ReduceFinished.RUnlock()
		close(c.MapTask)
		close(c.ReduceTask)
		time.Sleep(time.Second)
		ret = true
		return ret
	}
	c.mu_MapFinished.RUnlock()
	c.mu_ReduceFinished.RUnlock()

	return ret
}

func (c *Coordinator) addReduceTask(nReduce int) {
	for {
		c.mu_MapFinished.Lock()
		if c.MapFinished {
			c.mu_MapFinished.Unlock()
			break
		}
		c.mu_MapFinished.Unlock()
		time.Sleep(time.Second)
	}
	for i := 1; i <= nReduce; i++ {
		c.ReduceTask <- i
		c.mu_WaitReduce.Lock()
		c.WaitReduce[i] = true
		c.mu_WaitReduce.Unlock()
	}
}

func (c *Coordinator) addMapTask(files []string) {
	for i, file := range files {
		c.MapTask <- mapid{file, i + 1}
		c.mu_WaitMap.Lock()
		c.WaitMap[i+1] = true
		// 放中间会导致 waitmap 一直被锁
		c.mu_WaitMap.Unlock()
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.MapTask = make(chan mapid)
	c.ReduceTask = make(chan int)
	c.nMapTask = len(files)
	c.nReduceTask = nReduce
	c.NReduce = nReduce
	c.MapFinished = false
	c.ReduceFinished = false
	c.WaitMap = make(map[int]bool)
	c.WaitReduce = make(map[int]bool)

	// Your code here.

	go c.addMapTask(files)

	go c.addReduceTask(nReduce)

	// os.Create("./log.txt")

	c.server()
	return &c
}
