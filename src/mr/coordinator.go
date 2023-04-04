package mr

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const Debug = false

func DebugPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

type Coordinator struct {
	// Your definitions here.
	nReduce   int
	files     []string
	taskId    int
	phase     TaskPhase
	taskSet   map[int]*Task
	taskQueue chan int
	mu        sync.Mutex
}

func (c *Coordinator) initMapTasks() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, file := range c.files {
		c.generateMapTask(c.taskId, file)
		c.taskId++
	}
	DebugPrintf("Coordinator Init %v Map tasks\n", len(c.taskQueue))
}

func (c *Coordinator) generateMapTask(taskId int, file string) {
	task := Task{
		Id:        taskId,
		Type:      MapTask,
		Stat:      ReadyStat,
		Deadline:  -1,
		ReduceNum: c.nReduce,
		Files:     []string{file},
	}
	c.taskQueue <- task.Id
	c.taskSet[taskId] = &task
	DebugPrintf("Coordinator generate Map task : %v\n", task)
}

func (c *Coordinator) initReduceTasks() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i := 0; i < c.nReduce; i++ {
		c.generateReduceTask(c.taskId, i)
		c.taskId++
	}
	DebugPrintf("Coordinator Init %v Reduce tasks\n", c.nReduce)
}

func (c *Coordinator) generateReduceTask(taskId int, reduceId int) {
	task := Task{
		Id:        taskId,
		Type:      ReduceTask,
		Stat:      ReadyStat,
		Deadline:  -1,
		ReduceNum: c.nReduce,
		Files:     []string{},
	}
	for mapId := 0; mapId < len(c.files); mapId++ {
		task.Files = append(task.Files, reduceTmpFileName(mapId, reduceId))
	}
	c.taskQueue <- task.Id
	c.taskSet[taskId] = &task
	DebugPrintf("Coordinator generate Reduce task : %v\n", task)
}

func (c *Coordinator) schedule() {

	if c.phase == DonePhase {
		return
	}

	c.refreshTimeoutTask()

	allDone := c.isAllDone()

	DebugPrintf("Coordinator schedule: Phase:%v, allDone:%v\n", c.phase, allDone)

	if allDone {
		c.nextPhase()
	}
}

func (c *Coordinator) refreshTimeoutTask() {
	for _, task := range c.taskSet {
		if task.Stat == RunningStat && task.Deadline != -1 && time.Now().Unix() > task.Deadline {
			// timeout
			task.Deadline = -1
			task.Stat = ReadyStat
			c.taskQueue <- task.Id
		}
	}
}

func (c *Coordinator) isAllDone() bool {
	allDone := true
	for _, task := range c.taskSet {
		switch task.Stat {
		case ReadyStat:
			allDone = false
		case RunningStat:
			allDone = false
		case ErrorStat:
			allDone = false
			task.Stat = ReadyStat
			c.taskQueue <- task.Id
		case FinishStat:
		default:
		}
	}
	return allDone
}

func (c *Coordinator) nextPhase() {
	if c.phase == MapPhase {
		c.phase = ReducePhase
		c.initReduceTasks()
	} else if c.phase == ReducePhase {
		c.phase = DonePhase
	}
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (c *Coordinator) GetTask(args *ExampleArgs, reply *[]byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var task *Task

	if len(c.taskQueue) > 0 {
		taskId := <-c.taskQueue
		task = c.taskSet[taskId]
		task.Deadline = time.Now().Add(TIME_OUT).Unix()
		task.Stat = RunningStat

	} else if c.phase == DonePhase {
		task = &Task{
			Id:   c.taskId,
			Type: DoneTask,
		}
		c.taskId++

	} else {
		task = &Task{
			Id:   c.taskId,
			Type: WaitTask,
		}
		c.taskId++
	}

	data, err := json.Marshal(task)
	if err != nil {
		DebugPrintf("Coordinator GetTask Marshal err: %v\n", err)
		return err
	} else {
		DebugPrintf("Coordinator GetTask: %v\n", string(data))
	}

	*reply = data
	return nil
}

func (c *Coordinator) ReportTask(args *Task, reply *ExampleReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.Type == MapTask || args.Type == ReduceTask {
		c.taskSet[args.Id].Stat = args.Stat
	}

	go c.schedule()
	return nil
}

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
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	return c.phase == DonePhase
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		nReduce:   nReduce,
		files:     files,
		taskId:    0,
		phase:     MapPhase,
		taskSet:   map[int]*Task{},
		taskQueue: make(chan int, max(len(files), nReduce)),
		mu:        sync.Mutex{},
	}

	c.initMapTasks()
	c.server()
	return &c
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}
