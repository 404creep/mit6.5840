package raft

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

// Debugging
const Debug = true

// raft state
type State int

// 定义状态常量
const (
	Follower State = iota
	Candidate
	Leader
)

// 为 State 类型实现 String() 方法
func (s *State) String() string {
	switch *s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
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
		s := fmt.Sprintf("server:%v(%v term=%v): ", rf.me, rf.status.state.String(), rf.status.CurrentTerm)
		s += format + "\n"
		log.Printf(s, args...)
	}
}

func (rf *Raft) heartbeatTimer() {
	HeartbeatTimeout := time.Duration(100) * time.Millisecond
	rf.timer = time.After(HeartbeatTimeout)
}

func (rf *Raft) electionTimer() {
	ElectionTimeOut := time.Duration(300+rand.Intn(150)) * time.Millisecond
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
	lastIndex := rf.LastLogEntry().LogIndex
	lastTerm := rf.LastLogEntry().Term
	return msgLastLogTerm > lastTerm || (msgLastLogTerm == lastTerm && msgLastLogIndex >= lastIndex)
}

func (rf *Raft) findFirstLogIndexByTerm(term int) int {
	for _, l := range rf.status.Logs {
		if l.Term == term {
			return l.LogIndex
		}
	}
	return -1
}

// return CurrentTerm and whether this server believes it is the leader.
// 测试代码里面会调用raft.go中的getState方法，判断你当前的任期和是否是领导人
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	return rf.status.CurrentTerm, rf.status.state == Leader
}

func (rf *Raft) LastLogEntry() LogEntry {
	return rf.status.Logs.LastLogEntry()
}
