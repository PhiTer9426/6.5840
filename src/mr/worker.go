package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		task, err := getTask()
		if err != nil {
			panic("Worker getTask err\n")
		}

		switch task.Type {
		case MapTask:
			doMapTask(mapf, &task)
		case ReduceTask:
			doReduceTask(reducef, &task)
		case WaitTask:
			fmt.Printf("Worker: wait 3 second\n")
			time.Sleep(time.Second * 3)
			task.Stat = FinishStat
			reportTask(&task)
		case DoneTask:
			fmt.Printf("Worker: will be closed\n")
			task.Stat = FinishStat
			reportTask(&task)
			return
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func doMapTask(mapf func(string, string) []KeyValue, task *Task) {
	if len(task.Files) < 1 {
		task.Stat = ErrorStat
		reportTask(task)
		panic("Worker doMapTask: map task invalid\n")
		return
	}
	contents, err := ioutil.ReadFile(task.Files[0])
	if err != nil {
		task.Stat = ErrorStat
		reportTask(task)
		return
	}

	kvs := mapf(task.Files[0], string(contents))

	reduces := make([][]KeyValue, task.ReduceNum)
	for _, kv := range kvs {
		idx := ihash(kv.Key) % task.ReduceNum
		reduces[idx] = append(reduces[idx], kv)
	}

	for reduceId, reduce := range reduces {
		// create tmp out file
		fileName := reduceTmpFileName(task.Id, reduceId)
		file, err := os.Create(fileName)
		if err != nil {
			task.Stat = ErrorStat
			reportTask(task)
			return
		}

		// write file
		enc := json.NewEncoder(file)
		for _, kv := range reduce {
			if err := enc.Encode(&kv); err != nil {
				task.Stat = ErrorStat
				reportTask(task)
				return
			}
		}

		// close file
		if err := file.Close(); err != nil {
			task.Stat = ErrorStat
			reportTask(task)
			return
		}
	}
	task.Stat = FinishStat
	reportTask(task)
}

func doReduceTask(reducef func(string, []string) string, task *Task) {
	if len(task.Files) < 1 {
		task.Stat = ErrorStat
		reportTask(task)
		return
	}

	reducerId := task.Id
	intermediate := merge(task.Files)
	dir, _ := os.Getwd()

	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		task.Stat = ErrorStat
		reportTask(task)
		return
	}

	values := map[string][]string{}
	for i := 0; i < len(intermediate); i++ {
		values[intermediate[i].Key] = append(values[intermediate[i].Key], "1")
	}

	keys := make([]string, 0)
	for k, _ := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, key := range keys {
		cnt := reducef(key, values[key])
		_, err := fmt.Fprintf(tempFile, "%v %v\n", key, cnt)
		if err != nil {
			task.Stat = ErrorStat
			reportTask(task)
			return
		}
	}

	tempFile.Close()
	os.Rename(tempFile.Name(), fmt.Sprintf("mr-out-%d", reducerId))
	task.Stat = FinishStat
	reportTask(task)
}

func merge(files []string) []KeyValue {
	var kvs []KeyValue
	for _, filepath := range files {
		file, _ := os.Open(filepath)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
		file.Close()
	}
	return kvs
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//

func getTask() (Task, error) {

	fmt.Printf("Worker try get a task\n")
	reply := []byte{}
	args := ExampleArgs{}
	ok := call("Coordinator.GetTask", &args, &reply)

	//wMu.Unlock()
	if ok {
		fmt.Printf("Worker getTask: %v\n", string(reply))
	} else {
		fmt.Printf("call failed!\n")
	}

	task := Task{}
	err := json.Unmarshal(reply, &task)

	return task, err

}

func reportTask(task *Task) {

	reply := ExampleReply{}
	ok := call("Coordinator.ReportTask", &task, &reply)
	//wMu.Unlock()
	if ok {
		fmt.Printf("Worker reportTask: Type:%v, Id:%v, Stat:%v\n", task.Type, task.Id, task.Stat)
	} else {
		fmt.Printf("call failed!\n")
	}
}

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
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
