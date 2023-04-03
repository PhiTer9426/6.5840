package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskPhase int

const (
	MapPhase    TaskPhase = 0
	ReducePhase TaskPhase = 1
	DonePhase   TaskPhase = 2
)

type TaskType int

const (
	MapTask    TaskType = 0
	ReduceTask TaskType = 1
	DoneTask   TaskType = 2
	WaitTask   TaskType = 3
)

type TaskStat int

const (
	ReadyStat   TaskStat = 0
	RunningStat TaskStat = 1
	FinishStat  TaskStat = 2
	ErrorStat   TaskStat = 3
)

type Task struct {
	Id        int      `json:"id"`
	Type      TaskType `json:"type"`
	Stat      TaskStat `json:"stat"`
	ReduceNum int      `json:"reduce_num"`
	Files     []string `json:"files"`
}

func reduceTmpFileName(mapId, reduceId int) string {
	return "mr-tmp-" + strconv.Itoa(mapId) + "-" + strconv.Itoa(reduceId)
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
