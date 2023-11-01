package mr

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var mu sync.Mutex

type Job struct {
	JobType    int      //让worker知道任务类型 map/reduce
	JobId      int      //任务的Id
	ReducerNum int      //reducer的数量，用于hash
	InputFile  []string //输入文件的切片，map对应一个文件，reduce对应多个temp中间文件
}

type JobMetaInfo struct {
	condition int       //任务的状态
	StartTime time.Time //任务的开始时间，为crash准备
	JobPtr    *Job      //job 内存地址
}

type JobMetaHolder struct {
	MetaMap map[int]*JobMetaInfo
}

type Coordinator struct {
	// Your definitions here.
	JobChannelMap    chan *Job
	JobChannelReduce chan *Job
	MapNum           int
	ReduceNum        int
	CoordinatorPhase int // 目前整个框架应该处于什么任务阶段
	uniqueJobId      int
	JobMetaHolder    JobMetaHolder
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		JobChannelMap:    make(chan *Job, len(files)),
		JobChannelReduce: make(chan *Job, nReduce),
		JobMetaHolder: JobMetaHolder{
			MetaMap: make(map[int]*JobMetaInfo, len(files)+nReduce),
		},
		CoordinatorPhase: MapPhase,
		ReduceNum:        nReduce,
		MapNum:           len(files),
		uniqueJobId:      0,
	}

	// Your code here.
	c.makeMapJobs(files)

	//使 Coordinator 成为一个可以接受远程调用的 RPC 服务器
	c.server()

	go c.CrashDetector()
	return &c
}

// 用于检测任务是否崩溃, 并在必要时重新分配任务
func (c *Coordinator) CrashDetector() {
	for {
		time.Sleep(time.Second * 2)
		mu.Lock()
		if c.CoordinatorPhase == AllDone {
			mu.Unlock()
			break
		}

		for _, v := range c.JobMetaHolder.MetaMap {

			if v.condition == JobWorking && time.Since(v.StartTime) > 9*time.Second {
				fmt.Printf("the job[ %d ] is crash,take [%d] s\n", v.JobPtr.JobId, time.Since(v.StartTime))

				switch v.JobPtr.JobType {
				case MapJob:
					c.JobChannelMap <- v.JobPtr
					v.condition = JobWaiting
				case ReduceJob:
					c.JobChannelReduce <- v.JobPtr
					v.condition = JobWaiting
				}
			}
		}
		mu.Unlock()
	}
}

// 通过结构体的TaskId自增来获取唯一的任务id
func (c *Coordinator) generateJobsId() int {
	res := c.uniqueJobId
	c.uniqueJobId++
	return res
}

// 初始化map任务信息,，元信息放入JobMetaHolder， 放入通道中等待worker处理
func (c *Coordinator) makeMapJobs(files []string) {
	for _, v := range files {
		id := c.generateJobsId()
		// 新建MapJob
		job := Job{
			JobType:    MapJob,
			InputFile:  []string{v},
			JobId:      id,
			ReducerNum: c.ReduceNum,
		}
		// 新建Job的初始信息
		JobMetaInfo := JobMetaInfo{
			condition: JobWaiting, // 任务等待被执行
			JobPtr:    &job,       // 保存任务的地址
		}
		c.JobMetaHolder.acceptMeta(&JobMetaInfo)
		c.JobChannelMap <- &job
	}
	fmt.Println("Coordinator finished making map jobs")
}

// 分配reduce worker处理文件，初始化reduce任务信息,，元信息放入JobMetaHolder， 放入通道中等待worker处理
func (c *Coordinator) makeReduceJobs() {
	for i := 0; i < c.ReduceNum; i++ {
		id := c.generateJobsId()
		job := Job{
			JobType:   ReduceJob,
			InputFile: AssignReduceFile(i),
			JobId:     id,
		}
		JobMetaInfo := JobMetaInfo{
			condition: JobWaiting,
			JobPtr:    &job,
		}
		c.JobMetaHolder.acceptMeta(&JobMetaInfo)
		c.JobChannelReduce <- &job
	}
	fmt.Println("Coordinator finished making reduce jobs")
}

func AssignReduceFile(whichReduce int) []string {
	var reducefile []string
	path, _ := os.Getwd()
	rd, _ := os.ReadDir(path)
	for _, file := range rd {
		if strings.HasPrefix(file.Name(), "mr-tmp") && strings.HasSuffix(file.Name(), strconv.Itoa(whichReduce)) {
			reducefile = append(reducefile, file.Name())
		}
	}
	return reducefile
}

// 把任务信息JobMetaInfo 放入 JobMetaHolder，
func (j *JobMetaHolder) acceptMeta(JobInfo *JobMetaInfo) bool {
	JobId := JobInfo.JobPtr.JobId
	meta, _ := j.MetaMap[JobId]
	if meta != nil {
		fmt.Println("meta contains job which id = ", JobId)
		return false
	} else {
		j.MetaMap[JobId] = JobInfo
	}
	return true
}

// 判断给定任务是否在工作，并修正其目前任务信息状态（如果任务不在工作 更改任务状态 并 返回true）
func (j *JobMetaHolder) judgeJobState(JobId int) bool {
	meta, ok := j.MetaMap[JobId]
	if !ok || meta.condition != JobWaiting {
		return false
	}
	meta.condition = JobWorking
	meta.StartTime = time.Now()
	return true //成功触发作业
}

// coordinator get a request from worker   worker一直请求 协调者 分发任务
func (c *Coordinator) DistributeJob(args *JobArgs, reply *Job) error {
	// 分发任务应该上锁，防止多个worker竞争，并用defer回退解锁
	mu.Lock()
	defer mu.Unlock()

	switch c.CoordinatorPhase {
	case MapPhase:
		{
			if len(c.JobChannelMap) > 0 {
				*reply = *<-c.JobChannelMap
				if len(reply.InputFile) == 0 {
					fmt.Printf("Distribute wrong map Job id [%d]\n", reply.JobId)
				}

				if !c.JobMetaHolder.judgeJobState(reply.JobId) {
					fmt.Printf("Mapjob job %d is running\n", reply.JobId)
				}
			} else { // 如果map任务被分发完了但是又没完成，此时就将任务设为Waiting
				reply.JobType = WaittingJob
				if c.JobMetaHolder.checkJobDone() {
					fmt.Println("CoordinatorPhase -> ReducePhase")
					c.nextPhase()
				}
				return nil
			}
		}
	case ReducePhase:
		{
			if len(c.JobChannelReduce) > 0 {
				*reply = *<-c.JobChannelReduce
				if len(reply.InputFile) == 0 {
					fmt.Printf("Distribute wrong reduce Job id [%d]\n", reply.JobId)
				}

				if !c.JobMetaHolder.judgeJobState(reply.JobId) {
					fmt.Printf("[duplicated Reducejob id]  %d is running\n", reply.JobId)
				}
			} else { // 如果reduce任务被分发完了但是又没完成，将reply的任务设为Waitting，候补worker
				reply.JobType = WaittingJob
				if c.JobMetaHolder.checkJobDone() {
					c.nextPhase()
				}
				return nil
			}
		}
	case AllDone:
		{
			reply.JobType = KillJob
		}
	default:
		panic("The phase undefined ! ! !")
	}
	return nil
}

// 检查当前阶段的任务是否完成，完成就nextPhase
func (j *JobMetaHolder) checkJobDone() bool {

	reduceDoneNum := 0
	reduceUndoneNum := 0
	mapDoneNum := 0
	mapUndoneNum := 0

	// 遍历协调器中储存task信息的map
	for _, v := range j.MetaMap {
		if v.JobPtr.JobType == MapJob {
			if v.condition == JobDone {
				mapDoneNum += 1
			} else {
				mapUndoneNum++
			}
		} else {
			if v.condition == JobDone {
				reduceDoneNum += 1
			} else {
				reduceUndoneNum++
			}
		}
	}
	fmt.Printf("%d/%d map jobs are done, %d/%d reduce job are done\n",
		mapDoneNum, mapDoneNum+mapUndoneNum, reduceDoneNum, reduceDoneNum+reduceUndoneNum)

	// 如果某一个map或者reduce全部做完了，代表需要切换下一阶段，返回true
	// xxxDoneNum 是初步判断处于什么阶段， xxxUndoneNUm 是判断该阶段的工作是否完成
	if (mapDoneNum > 0 && mapUndoneNum == 0) && (reduceDoneNum == 0 && reduceUndoneNum == 0) { //map阶段且工作结束
		return true
	} else { // 处于reduce阶段
		if reduceDoneNum > 0 && reduceUndoneNum == 0 { //reduce工作结束
			return true
		}
	}
	return false
}

// 当channel长度为0时，去checkJobDone 检查，当这个阶段的任务都完成转换状态
func (c *Coordinator) nextPhase() {
	if c.CoordinatorPhase == MapPhase {
		c.makeReduceJobs()
		c.CoordinatorPhase = ReducePhase
	} else if c.CoordinatorPhase == ReducePhase {
		c.CoordinatorPhase = AllDone
	}
}

// start a thread that listens for RPCs from worker.go
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

// callDone调用 ，rpc在协调者中将任务状态为设为已完成，以方便协调者确认任务已完成
func (c *Coordinator) MarkJobDone(args *Job, reply *Job) error {
	mu.Lock()
	defer mu.Unlock()
	switch args.JobType {
	case MapJob:
		meta, ok := c.JobMetaHolder.MetaMap[args.JobId]
		//prevent a duplicated work which returned from another worker
		if ok && meta.condition == JobWorking {
			meta.condition = JobDone
		} else {
			fmt.Printf("map job [%d] is mark finished,already !!\n", args.JobId)
		}
		break
	case ReduceJob:
		meta, ok := c.JobMetaHolder.MetaMap[args.JobId]
		if ok && meta.condition == JobWorking {
			meta.condition = JobDone
		} else {
			fmt.Printf("reduce job [%d] is mark finished,already !!\n", args.JobId)
		}
		break
	default:
		panic("A undefined job is done !!")
	}
	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	if c.CoordinatorPhase == AllDone {
		fmt.Println("All jobs are finished, the coordinator will be exit!!")
		return true
	} else {
		return false
	}
}
