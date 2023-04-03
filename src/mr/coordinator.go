package mr

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	nReduce   int
	files     []string
	taskId    int
	phase     TaskPhase
	taskStats map[int]*Task
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
	fmt.Printf("Coordinator Init %v Map tasks\n", len(c.taskQueue))
}

func (c *Coordinator) generateMapTask(taskId int, file string) {
	task := Task{
		Id:        taskId,
		Type:      MapTask,
		Stat:      ReadyStat,
		ReduceNum: c.nReduce,
		Files:     []string{file},
	}
	c.taskQueue <- task.Id
	c.taskStats[taskId] = &task
	fmt.Printf("Coordinator generate Map task : %v\n", task)
}

func (c *Coordinator) initReduceTasks() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i := 0; i < c.nReduce; i++ {
		c.generateReduceTask(c.taskId, i)
		c.taskId++
	}
	fmt.Printf("Coordinator Init %v Reduce tasks\n", c.nReduce)
}

func (c *Coordinator) generateReduceTask(taskId int, reduceId int) {
	task := Task{
		Id:        taskId,
		Type:      ReduceTask,
		Stat:      ReadyStat,
		ReduceNum: c.nReduce,
		Files:     []string{},
	}
	for mapId := 0; mapId < len(c.files); mapId++ {
		task.Files = append(task.Files, reduceTmpFileName(mapId, reduceId))
	}
	c.taskQueue <- task.Id
	c.taskStats[taskId] = &task
	fmt.Printf("Coordinator generate Reduce task : %v\n", task)
}

func (c *Coordinator) schedule() {

	if c.phase == DonePhase {
		return
	}

	allDone := true
	for _, task := range c.taskStats {
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

	fmt.Printf("Coordinator schedule: Phase:%v, allDone:%v\n", c.phase, allDone)

	if allDone {
		if c.phase == MapPhase {
			c.phase = ReducePhase
			c.initReduceTasks()
		} else if c.phase == ReducePhase {
			c.phase = DonePhase
		}
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
		task = c.taskStats[taskId]
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
		fmt.Printf("Coordinator GetTask Marshal err: %v\n", err)
		return err
	} else {
		fmt.Printf("Coordinator GetTask: %v\n", string(data))
	}

	*reply = data
	return nil
}

func (c *Coordinator) ReportTask(args *Task, reply *ExampleReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.Type == MapTask || args.Type == ReduceTask {
		c.taskStats[args.Id].Stat = args.Stat
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	return c.phase == DonePhase
}

//
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
		taskStats: map[int]*Task{},
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
