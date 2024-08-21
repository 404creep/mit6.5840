package raft

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

// Debugging
const Debug = false

// raft state
const (
	Follower = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term     int
	LogIndex int
	Command  interface{}
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

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

type UnboundedQueue struct {
	mu       sync.Mutex
	notEmpty *sync.Cond
	queue    []interface{}
}

// NewUnboundedQueue 创建一个新的 UnboundedQueue 实例
func NewUnboundedQueue() *UnboundedQueue {
	uq := &UnboundedQueue{
		queue: make([]interface{}, 0),
	}
	uq.notEmpty = sync.NewCond(&uq.mu)
	return uq
}

// Enqueue 将一个元素添加到队列中
func (uq *UnboundedQueue) Enqueue(item interface{}) {
	uq.mu.Lock()
	uq.queue = append(uq.queue, item)
	uq.mu.Unlock()
	uq.notEmpty.Broadcast() // 通知所有等待队列非空的 goroutine
}

// DequeueAll 返回队列中的所有元素，并清空队列
func (uq *UnboundedQueue) DequeueAll() []interface{} {
	uq.mu.Lock()
	defer uq.mu.Unlock()

	for len(uq.queue) == 0 {
		uq.notEmpty.Wait() // 如果队列为空，等待
	}

	items := uq.queue
	uq.queue = make([]interface{}, 0) // 清空队列
	return items
}

// ---------------------------------------

func (rf *Raft) debug(format string, args ...interface{}) {
	if Debug {
		s := fmt.Sprintf("%v(%v term=%v): ", rf.me, rf.status.state, rf.status.currentTerm)
		s += format + "\n"
		log.Printf(s, args...)
	}
}

func (rf *Raft) heartbeatTimer() {
	HeartbeatTimeout := time.Duration(50) * time.Millisecond
	rf.timer = time.After(HeartbeatTimeout)
}

func (rf *Raft) electionTimer() {
	ElectionTimeOut := time.Duration(150+rand.Intn(200)) * time.Millisecond
	rf.timer = time.After(ElectionTimeOut)
}

func (rf *Raft) resetLeaderTimer() {
	rf.heartbeatTimer()
}

func (rf *Raft) timerTimeOut() {
	rf.timer = time.After(0)
}

// 在处理别节点发来的RequestVote RPC时，需要做一些检查才能投出赞成票
// 1. 候选人最后一条Log条目的任期号大于本地最后一条Log条目的任期号；
// 2. 或者，候选人最后一条Log条目的任期号等于本地最后一条Log条目的任期号，且候选人的Log记录长度大于等于本地Log记录的长度
func (rf *Raft) isLogUpToDate(msgLastLogIndex int, msgLastLogTerm int) bool {
	lastIndex := rf.getLastLogIndex()
	lastTerm := rf.getLastLogTerm()
	return msgLastLogTerm > lastTerm || (msgLastLogTerm == lastTerm && msgLastLogIndex >= lastIndex)
}

func (rf *Raft) isMatchPrevLog(index int, term int) bool {
	return index >= len(rf.status.logs) || rf.status.logs[index].Term != term
}

func (rf *Raft) findFirstLogIndexByTerm(term int) int {
	for _, l := range rf.status.logs {
		if l.Term == term {
			return l.LogIndex
		}
	}
	return -1
}

func (rf *Raft) getLogByIndex(index int) (LogEntry, bool) {
	offsetIndex := index - rf.status.logs[0].LogIndex
	if offsetIndex < 0 || offsetIndex >= len(rf.status.logs) {
		return LogEntry{}, false
	}
	return rf.status.logs[offsetIndex], true
}

// 获取最后的下标(代表已存储），lastIdx可能因为快照而改变
func (rf *Raft) getLastLogIndex() int {
	lastIdx := len(rf.status.logs) - 1
	return rf.status.logs[lastIdx].LogIndex
}

// 获取最后的任期
func (rf *Raft) getLastLogTerm() int {
	if len(rf.status.logs) == 0 {
		return 0
	} else {
		return rf.status.logs[len(rf.status.logs)-1].Term
	}
}

// return currentTerm and whether this server believes it is the leader.
// 测试代码里面会调用raft.go中的getState方法，判断你当前的任期和是否是领导人
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	return rf.status.currentTerm, rf.status.state == Leader
}
