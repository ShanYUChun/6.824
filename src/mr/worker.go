package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// borrow form sequntial sortkeyvalue

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// task struct
type Task struct {
	Ttype string
	Fname string
	Tid   int
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	// constantly ask for task,if there is no task then sleep
	for {
		task, nReduce := getTask()
		if task == nil {
			return
		} else if task.Ttype == "Sleep" {
			// fmt.Printf("Sleep")
			time.Sleep(time.Second)
		} else if task.Ttype == "Finished" {
			return
		} else if task.Ttype == "Map" {
			// when task finished sendmessage back
			// fmt.Printf("Map Task %v doing ...", task.Tid)
			MapTask(task.Fname, nReduce, task.Tid, mapf)
			// fmt.Printf("Map Task %v done ...", task.Tid)
			SendBack("Map", task.Fname, task.Tid)
		} else if task.Ttype == "Reduce" {
			// fmt.Printf("Reduce Task %v doing ...", task.Tid)
			ReduceTask(task.Fname, task.Tid, reducef)
			// fmt.Printf("Reduce Task %v done ...", task.Tid)
			SendBack("Reduce", task.Fname, task.Tid)
		}
	}

}

func getTask() (*Task, int) {

	// arguments Args in rpc.go
	args := GetArgs{}
	// set arg in args

	// reply should be zero-allocated
	reply := GetReply{}

	ok := call("Coordinator.SendTask", &args, &reply)
	if ok {
		return &reply.Rtask, reply.NReduce
	} else {
		return nil, 0
	}
}

func MapTask(filename string, nReduce int, id int, mapf func(string, string) []KeyValue) {
	// reading file
	// todo ... handle read error
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open file:%v", filename)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read file:%v", filename)
	}

	file.Close()
	// read end

	// do map
	kva := mapf(filename, string(content))
	// end map

	// write kva into json file

	// create a json Temporary file
	// give each of them a jsonencoder
	// strconv.Itoa(id)+strconv.Itoa(i)
	encs := make([]*json.Encoder, nReduce)
	// go 里 变量 无法创建静态数组，所以只创建动态数组，并且 也没有 const
	tempname := make([]string, nReduce)
	for i := 1; i <= nReduce; i++ {
		// test 不再 main 这个文件夹会失效，就在当前文件夹下面
		tempfile, _ := ioutil.TempFile("", "out-*.json")
		// will return error message but ignored there
		encs[i-1] = json.NewEncoder(tempfile)
		tempname[i-1] = tempfile.Name()
	}
	// this implementation is not apply sort to Map, but there should have one
	// will add sort latter

	for i := range kva {
		hashkey := ihash(kva[i].Key) % nReduce
		encs[hashkey].Encode(&kva[i])
		// this err should get handle
	}

	// file rename
	for i, temp := range tempname {
		intername := "mr-" + strconv.Itoa(id) + "-" + strconv.Itoa(i+1) + ".json"
		if _, err := os.Stat(intername); err != nil {
			os.Rename(temp, intername)
		} else {
			os.Remove(temp)
		}
	}

}

func ReduceTask(dirpath string, id int, reducef func(string, []string) string) {
	// this function will return all output file for this reduce job
	files, err := getAllFiles(dirpath, id)
	if err != nil {
		log.Fatalf("cannot open dirctory:%v", dirpath)
	}

	kva := []KeyValue{}
	for _, filename := range files {
		file, _ := os.Open(filename)
		// there will be path error
		dec := json.NewDecoder(file)
		// read all kv in file
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	sort.Sort(ByKey(kva))

	i := 0

	oname := "mr-out-" + strconv.Itoa(id)

	// handle the err
	tempout, err := ioutil.TempFile("", "*.txt")

	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}

		output := reducef(kva[i].Key, values)

		fmt.Fprintf(tempout, "%v %v\n", kva[i].Key, output)
		i = j
	}

	// 不需要 锁
	// 如果两个进程同时完成 Task，都判断没有该文件，那么后 rename的文件就是 会覆盖前一个rename文件
	// 设置 coordinator 在 maptask 完成之后 sleep ，再分发 reduce task，可以保证 在reduce task 读文件时，不会发生 冲突
	if _, err := os.Stat(oname); err != nil {
		os.Rename(tempout.Name(), oname)
	} else {
		os.Remove(tempout.Name())
	}
}

func SendBack(task string, file string, id int) {
	// tell coordinator task has finished
	args := SendArgs{task, id}
	reply := SendReply{}
	// fmt.Printf("task %v,%v sending to coordinator ", task, id)
	ok := call("Coordinator.TaskDone", &args, &reply)
	if ok {
		if reply.Message == "Accepted" {
			// fmt.Printf("task %v,%v has finished and recived\n", task, id)
		} else {
			// fmt.Printf("task %v,%v has finished but reject\n", task, id)
		}
	} else {
		// fmt.Printf("task %v,%v has finished and failed to connect\n", task, id)
	}
}

func getAllFiles(dirpath string, id int) ([]string, error) {
	var files []string
	allFile, err := ioutil.ReadDir(dirpath)
	if err != nil {
		// // fmt.Printf("%v connot open \n", dirpath)
		return nil, err
	}
	for _, file := range allFile {
		if strings.HasSuffix(file.Name(), strconv.Itoa(id)+".json") {
			files = append(files, path.Join(dirpath, file.Name()))
		}
	}

	return files, nil
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		// fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		// fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//

// interface{} in go == std::any in c++
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	// connect to the server add call Newclient can return that client
	// sock is the same with coordinator
	// unix 后面跟的应该是一个 文件 ，client 和 server 通过 文件 通信
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
