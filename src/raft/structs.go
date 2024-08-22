package raft

import (
	"6.5840/labrpc"
	"sync"
	"time"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	status Status           // 服务器的状态
	timer  <-chan time.Time // 用于接收计时器的信号

	// 外部消息, 进入总线
	messagePipeLine chan Message

	// start 传来的command
	commandChan  chan CommandInfo
	snapShotChan chan SnapShotInfo

	applyChan  chan ApplyMsg   // 	日志都是存在这里client取（2B），但是无缓冲
	applyQueue *UnboundedQueue // 防止applyMsg阻塞的缓冲队列
}

type Status struct {
	// 该节点是什么角色（状态）
	state State

	// 正常情况下commitIndex与lastApplied应该是一样的，但是如果有一个新的提交，并且还未应用的话last应该要更小些
	commitIndex int // 已知被 提交 的最大日志条目索引  (初始化为0，持续递增）
	lastApplied int // 已被状态机 执行 的最大日志条目索引

	CandidateInfo
	LeaderInfo
	PersistInfo
}

type PersistInfo struct {
	// 所有的servers拥有的变量:
	CurrentTerm       int  // 记录当前的任期
	VotedFor          int  // 记录当前的任期把票投给了谁
	Logs              Logs //  first index is 1 日志条目数组，包含了状态机要执行的指令集，以及收到领导时的任期号
	LastIncludedIndex int  //该索引以及之前的所有条目都已经被快照覆盖
	LastIncludedTerm  int
}
type CandidateInfo struct {
	receiveVoteNum int // 收到的选票数
}

type LeaderInfo struct {
	//这里的index都是数组下标，不是对应的logIndex
	nextIndex  []int // 对于每一个server，需要发送给他下一个日志条目的索引值（初始化为leader日志index+1,那么范围就对标len）
	matchIndex []int // 某follower的log与leader的log最大匹配到第几个Index,已经apply
}

type CommandInfo struct {
	Command  interface{}
	RespChan chan CommandRespInfo
}
type CommandRespInfo struct {
	Term     int
	Index    int
	IsLeader bool
}

type SnapShotInfo struct {
	Index    int
	SnapShot []byte
	RespChan chan struct{}
}

type InstallSnapshotArgs struct {
	Term             int    // 发送请求方的任期
	LeaderId         int    // 请求方的LeaderId
	LastIncludeIndex int    // 快照最后applied的日志下标
	LastIncludeTerm  int    // 快照最后applied时的当前任期
	Data             []byte // 快照区块的原始字节流数据
	//Done bool
}

type InstallSnapshotReply struct {
	Term int
}

type AppendEntriesReply struct {
	Term    int  // leader的term可能是过时的，此时收到的Term用于更新他自己
	Success bool //	如果follower与Args中的PreLogIndex/PreLogTerm都匹配才会接过去新的日志（追加），不匹配直接返回false
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	//For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
