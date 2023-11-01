package mr

import "os"
import "strconv"

// JobType
const (
	MapJob = iota
	ReduceJob
	WaittingJob // Waittingen任务代表此时为任务都分发完了，但是任务还没完成，阶段未改变
	KillJob     // exit
)

// JobCondition
const (
	JobWorking = iota
	JobWaiting // 此阶段在等待执行
	JobDone    // 此阶段已经做完
)

// CoordinatorCondition
const (
	MapPhase = iota
	ReducePhase
	AllDone
)

type JobArgs struct{}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
r.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
