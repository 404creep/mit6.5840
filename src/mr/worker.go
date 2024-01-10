package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	alive := true

	for alive {

		job := GetJob()
		switch job.JobType {
		case MapJob:
			{
				DoMapJob(mapf, &job)
				callMarkDone(&job)
			}
		case ReduceJob:
			{
				DoReduceJob(reducef, &job)
				callMarkDone(&job)
			}
		case WaittingJob:
			{
				fmt.Println(" get waitting")
				time.Sleep(time.Second * 5)
			}
		case KillJob:
			{
				time.Sleep(time.Second)
				fmt.Println("All tasks are Done , terminated...")
				alive = false
			}
		}
	}

	time.Sleep(time.Second)
	// uncomment to send the Example RPC to the coordinator.
}

// 调用RPC拉取Coordinator的任务
func GetJob() Job {
	args := JobArgs{}
	reply := Job{}
	ok := call("Coordinator.DistributeJob", &args, &reply)
	if ok {
		//fmt.Println("Worker get response", &reply)
	} else {
		//argsStr := fmt.Sprintf("%#v", args)   // 将 args 转换为字符串
		//replyStr := fmt.Sprintf("%#v", reply) // 将 reply 转换为字符串
		//fmt.Printf("\nworker 77 call failed: rpc: DistributeJob, args: %s, reply: %s\n", argsStr, replyStr)
	}
	return reply
}

// 插件编辑进来的mapf方法处理Map生成一组kv，然后写到 mr-tmp-x-x 文件中
func DoMapJob(mapf func(string, string) []KeyValue, response *Job) {
	var intermediate []KeyValue
	if response.InputFile == nil {
		fmt.Printf("get worng map job [%d]\n", response.JobId)
	}
	filename := response.InputFile[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	//获取content，作为mapf的参数
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	//mapf 返回一组kv结构体数组
	intermediate = mapf(filename, string(content))
	rn := response.ReducerNum

	// 创建一个长度为nReduce的二维切片
	HashedKV := make([][]KeyValue, rn)

	for _, kv := range intermediate {
		HashedKV[ihash(kv.Key)%rn] = append(HashedKV[ihash(kv.Key)%rn], kv)
	}
	for i := 0; i < rn; i++ {
		oname := "mr-tmp-" + strconv.Itoa(response.JobId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile) //创造一个json编码器将json数据写入ofile中
		for _, kv := range HashedKV[i] {
			enc.Encode(kv)
			if err != nil {
				return
			}
		}
		ofile.Close()
	}
}

func DoReduceJob(reducef func(string, []string) string, response *Job) {
	reduceFileNum := response.JobId
	if response.InputFile == nil {
		fmt.Printf("get worng reduce job[%d]\n", response.JobId)
	}
	intermediate := shuffle(response.InputFile)
	dir, _ := os.Getwd()
	tempFile, _ := os.CreateTemp(dir, "mr-tmp-*")
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	tempFile.Close()
	oname := fmt.Sprintf("mr-out-%d", reduceFileNum)
	os.Rename(tempFile.Name(), oname)

}

// json读取reduce的文件内容，洗牌得到一组排序好的kv数组
func shuffle(files []string) []KeyValue {
	var kva []KeyValue
	for _, filepath := range files {
		file, _ := os.Open(filepath)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))
	return kva
}

// 做完任务也需要调用rpc在协调者中将f 任务状态为设为已完成，以方便协调者确认任务已完成，worker与协调者程序能正常退出
func callMarkDone(f *Job) Job {

	args := f
	reply := Job{}

	ok := call("Coordinator.MarkJobDone", &args, &reply)

	if ok {
		//fmt.Printf("worker finish jobId[%d],jobType[%d]\n", args.JobId, args.JobType)
	} else {
		fmt.Printf("MarkJobDone id [%d]call failed!\n", args.JobId)
	}
	return reply
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)

	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	argsStr1 := fmt.Sprintf("%#v", args)   // 将 args 转换为字符串
	replyStr1 := fmt.Sprintf("%#v", reply) // 将 reply 转换为字符串
	fmt.Printf("\nworker after args: rpc: %s,  args: %s, reply: %s\n", rpcname, argsStr1, replyStr1)
	fmt.Printf("worker  rpc.DialHTTP call err: %v\n++++++++++++++++++++++++++++++\n", err)
	return false
}
