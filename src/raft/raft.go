package raft

import (
	"6.5840/labgob"
	"bytes"
	"log"
	"sort"
	"sync/atomic"
	//	"6.5840/labgob"
	"6.5840/labrpc"
)

func (rf *Raft) Step() {
	rf.electionTimer()
	for {
		select {
		case <-rf.timer:
			rf.handleTimeOut()
		default:
			select {
			case <-rf.timer:
				rf.handleTimeOut()
			// start 传来的command
			case commandInfo := <-rf.commandChan:
				if rf.killed() {
					rf.debug("dead")
					go func() {
						respChan := commandInfo.RespChan
						respChan <- CommandRespInfo{Term: -1, LogIndex: -1, IsLeader: false}
					}()
					return
				}
				switch rf.status.state {
				case Candidate, Follower:
					go func(term int) {
						respChan := commandInfo.RespChan
						respChan <- CommandRespInfo{Term: term, LogIndex: -1, IsLeader: false}
					}(rf.status.CurrentTerm)
				case Leader:
					rf.status.Logs = append(rf.status.Logs, LogEntry{
						rf.status.CurrentTerm,
						rf.LastLogEntry().LogIndex + 1, // 保证leader的logIndex递增+1
						commandInfo.Command,
					})
					rf.persist(nil)
					rf.debug("received command(%v), index would be %v, now logs is %v",
						commandInfo.Command, rf.LastLogEntry().LogIndex, rf.status.Logs)
					go func(term, logIndex int) {
						respChan := commandInfo.RespChan
						respChan <- CommandRespInfo{
							Term:     term,
							LogIndex: logIndex,
							IsLeader: true,
						}
					}(rf.status.CurrentTerm, rf.LastLogEntry().LogIndex)
					// 为了尽快同步日志并返回客户端, 需要让定时器尽快过期
					rf.timerTimeOut()
				}
			case snapshotInfo := <-rf.snapShotChan:
				rf.debug("get snapshot command,lastIncludedIndex=%v logs=%v",
					snapshotInfo.LastIncludedLogIndex, rf.status.Logs)
				// 收到log的snapshot命令, 需要进行日志裁减, 然后把新的snapshot持久化
				lastIncludedlogEntry, ok := rf.status.Logs.GetLogEntryByLogIndex(snapshotInfo.LastIncludedLogIndex)
				if !ok {
					rf.debug("get snapshot command, but not found log in logs")
					snapshotInfo.RespChan <- struct{}{}
					break
				}
				rf.status.Logs.TrimFrontLogs(lastIncludedlogEntry.LogIndex)
				rf.status.LastIncludedIndex = lastIncludedlogEntry.LogIndex
				rf.status.LastIncludedTerm = lastIncludedlogEntry.Term
				rf.persist(snapshotInfo.SnapShot)
				rf.debug("snapshot done,newLogs=%v", rf.status.Logs)
				snapshotInfo.RespChan <- struct{}{}
			case intput := <-rf.messagePipeLine:
				if rf.status.CurrentTerm < intput.Term {
					rf.debug("found self term < remote term, turning into follower of term %v", intput.Term)
					rf.status.CurrentTerm = intput.Term
					rf.status.state = Follower
					rf.status.VotedFor = -1
					rf.persist(nil)
				}
				switch msg := intput.Msg.(type) {
				case *requestVoteRequest:
					// 此处一定rf.term >= msg.term
					rf.debug("received RequestVoteRequest from %v, remote lastLog (index=%v, term=%v), remote term=%v",
						msg.CandidateId, msg.LastLogIndex, msg.LastLogTerm, msg.Term)
					if rf.status.state == Follower && rf.status.VotedFor == -1 &&
						rf.status.CurrentTerm == msg.Term && rf.isLogUpToDate(msg.LastLogIndex, msg.LastLogTerm) {
						rf.status.VotedFor = msg.CandidateId
						rf.persist(nil)
						rf.debug("vote for %v,reqTerm=%v,logs=%v", msg.CandidateId, msg.Term, rf.status.Logs)
						go rf.sendRequestVoteReplyToCandidate(true, rf.status.CurrentTerm, msg)
						// you should only restart your election timer if
						// a)you get an AppendEntries RPC from the current leader (i.e., if the term in the AppendEntries arguments is outdated, you should not reset your timer);
						// b) you are starting an election; or
						// c) you grant a vote to another peer.
						rf.electionTimer()
					} else {
						// 如果是rf仍然是candidate or leader, 说明rf.term >= msg.term, 那么拒绝投票
						// 或者是follower 且 (==mag.term(已经投过票了||日志不够新)  || rf.term > msg.term)
						rf.debug("reject vote,reqTerm=%v,logs=%v,voteFor=%v", msg.Term, rf.status.Logs, rf.status.VotedFor)
						go rf.sendRequestVoteReplyToCandidate(false, rf.status.CurrentTerm, msg)
					}
				case *requestVoteReply:
					// 1.如果是过去的消息, 直接无视
					// 2.自己已经不是candidate了, 无视
					if msg.ReqTerm != rf.status.CurrentTerm || rf.status.state != Candidate {
						break
					}
					if msg.VoteGranted {
						rf.status.receiveVoteNum++
						if rf.status.receiveVoteNum > len(rf.peers)/2 {
							rf.debug("received %v votes,be leader,peerNum:%v",
								rf.status.receiveVoteNum, len(rf.peers))
							rf.status.state = Leader
							/*
								(Reinitialized after election)
								nextIndex[]: for each server, index of the next log entry
								to send to that server (initialized to leader last log index + 1)
								Leader尝试发送最新的日志 方便快速同步
								matchIndex[]: for each server, index of highest log entry
								known to be replicated on server(initialized to 0, increases monotonically)
								Leader 认为该 Follower 还没有复制任何日志条目
							*/
							nextIndex := rf.LastLogEntry().LogIndex + 1
							for i := 0; i < len(rf.peers); i++ {
								rf.status.nextIndex[i] = nextIndex
								rf.status.matchIndex[i] = 0
							}
							rf.debug("be leader, logs=%v, nextIndex=%v, matchIndex=%v",
								rf.status.Logs, rf.status.nextIndex, rf.status.matchIndex)
							// 成为leader 直接超时，发送心跳
							rf.timerTimeOut()
						}
					}
					//调用Snapshot传来的请求
				case *installSnapshotRequest:
					rf.debug("receive InstallSnapshotRequest from %v, lastIncIdx=%v, lastIncTerm=%v, self logs=%v",
						msg.LeaderId, msg.LastIncludedIndex, msg.LastIncludedTerm, rf.status.Logs)
					// 进入case时候，rf.term >= msg.term
					if msg.Term < rf.status.CurrentTerm {
						rf.sendInstallSnapshotReplyToLeader(msg, rf.status.CurrentTerm, false)
						break
					}
					// 发来的快照相当于一次心跳
					if rf.status.state == Candidate {
						rf.status.state = Follower
					}
					rf.electionTimer()
					//If existing log entry has same index and term as snapshot’s last included entry, retain log entries following it and reply
					entry, ok := rf.status.Logs.GetLogEntryByLogIndex(msg.LastIncludedIndex)
					//已经包含快照条目，且任期号匹配 或 提交索引大于快照的最后包含索引,直接返回true
					if (ok && entry.Term == msg.LastIncludedTerm) || msg.LastIncludedIndex <= rf.status.commitIndex {
						rf.debug("already have snapshot,msg.lastIncIndex=%v,commitIndex=%v,logs=%v",
							msg.LastIncludedIndex, rf.status.commitIndex, rf.status.Logs)
						go rf.sendInstallSnapshotReplyToLeader(msg, rf.status.CurrentTerm, true)
						break
					}
					// 如果没有快照的最后日志，删除所有的日志，只保留快照
					rf.debug("dont have lastIncLog(%v),discard all log,save len(snapshot) %v", msg.LastIncludedIndex, len(msg.Snapshot))
					rf.applySnapshot(msg, []LogEntry{{
						Term:     msg.LastIncludedTerm,
						LogIndex: msg.LastIncludedIndex,
						Command:  nil,
					}})
				case *installSnapshotReply:
					rf.debug("receive installSnapshotReply from %v,success=%v,reqLastIncludedIndex=%v", msg.Id, msg.Success, msg.ReqLastIncludedIndex)
					if rf.status.state != Leader || !msg.Success || msg.ReqTerm != rf.status.CurrentTerm {
						break
					}
					//成功复制快照，尝试更新该 Follower 的 NextLogIndex 和 MatchIndex
					rf.status.nextIndex[msg.Id] = max(rf.status.nextIndex[msg.Id], msg.ReqLastIncludedIndex+1)
					rf.status.matchIndex[msg.Id] = rf.status.nextIndex[msg.Id] - 1
					if rf.status.nextIndex[msg.Id] <= rf.LastLogEntry().LogIndex {
						rf.debug("still send logs to %v, nextIndex(%v), rf.Log(%v)", msg.Id, rf.status.nextIndex[msg.Id], rf.status.Logs)
						rf.leaderSendLogs(msg.Id)
					}
					// 更新commitIndex
					rf.updateCommitIndex()
				case *appendEntriesRequest:
					// 进入case时候，rf.term >= msg.term
					// 拒绝过期leader日志
					if msg.Term < rf.status.CurrentTerm {
						go rf.sendAppendEntriesReplyToLeader(false, rf.status.CurrentTerm, msg, map[string]int{})
						break
					}
					entries := make([]LogEntry, 0)
					for _, entryBytes := range msg.Entries {
						entry, ok := BytesToLogEntry(entryBytes)
						if !ok {
							log.Fatalf("BytesToLogEntry failed")
						}
						entries = append(entries, entry)
					}
					// rf.term == msg.term
					// todo why重置并持久化状态
					if rf.status.state == Candidate {
						rf.status.state = Follower
					}
					rf.electionTimer()
					// 收到心跳
					if msg.Entries == nil {
						rf.debug("receive heartbeat from %v,logs=%v", msg.LeaderId, rf.status.Logs)
					} else {
						rf.debug("receive appendEntriesRequest from %v,remoteTerm=%v,len(entries)=%v,preLog(index=%v term=%v),selfLogs = %v",
							msg.LeaderId, msg.Term, len(msg.Entries), msg.PrevLogIndex, msg.PrevLogTerm, rf.status.Logs)
					}
					// 快速回退，不断回退到leader发来的preLogIndex的term
					// case 3 : 没有preLog, 返回XLen
					preLogEntry, ok := rf.status.Logs.GetLogEntryByLogIndex(msg.PrevLogIndex)
					if !ok {
						success := false
						// 已经快照执行了
						if msg.PrevLogIndex+len(msg.Entries) <= rf.status.commitIndex {
							success = true
						}
						go rf.sendAppendEntriesReplyToLeader(success, rf.status.CurrentTerm, msg, map[string]int{
							"ConflictTerm":     -1,
							"NextSendLogIndex": rf.LastLogEntry().LogIndex + 1,
						})
						rf.debug("dont have preLogEntry,done(%v) preLogIndex=%v,nextSendLogIndex=%v,rf.Log=%v",
							success, msg.PrevLogIndex, rf.LastLogEntry().LogIndex+1, rf.status.Logs)
						break
					}
					// 有该preLog，但是term不匹配
					if preLogEntry.Term != msg.PrevLogTerm {
						conflictTerm := preLogEntry.Term
						conflictIndex := rf.findFirstLogIndexByTerm(conflictTerm)
						go rf.sendAppendEntriesReplyToLeader(false, rf.status.CurrentTerm, msg, map[string]int{
							"ConflictIndex": conflictIndex,
							"ConflictTerm":  conflictTerm,
						})
						break
					}

					if msg.Entries != nil {
						// 如果已经有最后的日志且term相等，说明msg.Entries是重复的，过期reply,直接返回成功
						lastAppendEntry := entries[len(entries)-1]
						preLogIdx, _ := rf.LogIndex2Idx("get follower preLogIdx", preLogEntry.LogIndex)
						if lastEntry, ok := rf.status.Logs.GetLogEntryByLogIndex(lastAppendEntry.LogIndex); !ok || lastEntry.Term != lastAppendEntry.Term {
							// 否则，直接替换rf.Logs[preLogIndex+1:]为msg.Entries
							rf.status.Logs = append(rf.status.Logs[:preLogIdx+1], entries...)
						}
						rf.debug("rf.preLog is %v, msg.preLogIndex is %v,matched, rf.newLog is %v,reqLogLen(%v), msgEntries=%v",
							preLogEntry, msg.PrevLogIndex, rf.status.Logs, len(entries), entries)
					}
					rf.persist(nil)
					//执行新的已经commit日志
					if msg.LeaderCommit > rf.status.commitIndex {
						rf.debug("appendEntry done,commitIndex=%v,leaderCommit=%v", rf.status.commitIndex, msg.LeaderCommit)
						rf.applyCommittedLogs(msg.LeaderCommit)
					}
					go rf.sendAppendEntriesReplyToLeader(true, rf.status.CurrentTerm, msg, map[string]int{})
				case *appendEntriesReply:
					if rf.status.state != Leader || msg.Term < rf.status.CurrentTerm ||
						msg.ReqTerm != rf.status.CurrentTerm {
						break
					}
					// preLog已经快照了,直接发送新的快照
					if _, ok := rf.status.Logs.GetLogEntryByLogIndex(rf.status.nextIndex[msg.Id] - 1); !ok {
						rf.debug("dont find preLogIndex %v, self log is %v", rf.status.nextIndex[msg.Id], rf.status.Logs)
						rf.sendSnapShotRequestToFollower(msg.Id)
						break
					}
					if msg.ReqLogLen > 0 {
						rf.debug("receiveAppendEntriesReply from %v, done=%v,reqPreLogIndex=%v,reqLogLen=%v", msg.Id, msg.Success, msg.ReqPrevLogIndex, msg.ReqLogLen)
					}
					// 日志复制成功，更新nextIndex和matchIndex
					if msg.Success {
						// 防止过期reply回退nextIndex
						newNextLogIndex := msg.ReqPrevLogIndex + msg.ReqLogLen + 1
						if newNextLogIndex > rf.LastLogEntry().LogIndex+1 {
							rf.debug("get reply from %v,nextIndex(%v) is too large, preIndex(%v),lastIncIndex(%v),reqLogLen(%v), leaderLog is%v",
								msg.Id, newNextLogIndex, msg.ReqPrevLogIndex, rf.status.LastIncludedIndex, msg.ReqLogLen, rf.status.Logs)
							panic("check me")
						}
						rf.status.nextIndex[msg.Id] = max(rf.status.nextIndex[msg.Id], newNextLogIndex)
						rf.status.matchIndex[msg.Id] = rf.status.nextIndex[msg.Id] - 1
						if msg.ReqLogLen > 0 {
							rf.debug("appendEntry %v done,nextIndex=%v,preLogIndex=%v,reqLogLen=%v", msg.Id, rf.status.nextIndex, msg.ReqPrevLogIndex, msg.ReqLogLen)
						}
						rf.updateCommitIndex()
					} else {
						// 日志复制失败，回退nextIndex,重新发送
						// case 3 follower中日志太短,应该回退到lastLog的下一条
						if msg.ConflictTerm == -1 {
							nextLogEntry, ok := rf.status.Logs.GetLogEntryByLogIndex(msg.NextSendLogIndex)
							if ok {
								rf.status.nextIndex[msg.Id] = msg.NextSendLogIndex
								rf.debug("appendEntry %v failed, lastIndex=%v,nextSendLog is %v", msg.Id, msg.NextSendLogIndex-1, nextLogEntry)
							} else {
								// 找不到下一个日志，说明需要发送快照
								rf.debug("appendEntry %v failed, and dont find nextSendLogIndex %v,log is %v", msg.Id, msg.NextSendLogIndex, rf.status.Logs)
								rf.sendSnapShotRequestToFollower(msg.Id)
							}
						} else {
							firstLogIndexOfTerm := rf.findFirstLogIndexByTerm(msg.ConflictTerm)
							if firstLogIndexOfTerm > 0 {
								// case 2 有该任期日志
								rf.status.nextIndex[msg.Id] = firstLogIndexOfTerm + 1
							} else {
								//case 1 没有该任期的日志，nextIndex为该term的第一条日志，回退一整个term
								rf.status.nextIndex[msg.Id] = msg.ConflictIndex
							}
						}
						rf.leaderSendLogs(msg.Id)
					}
				}
			}
		}
	}
}

func (rf *Raft) handleTimeOut() {
	switch rf.status.state {
	// 如果是follower超时, 那么进入candidate状态
	case Follower:
		rf.debug("timeout,being candidate,logs=%v", rf.status.Logs)
		rf.status.state = Candidate
		rf.status.receiveVoteNum = 1
		rf.status.VotedFor = rf.me
		rf.status.CurrentTerm++
		rf.persist(nil)
		rf.electionTimer()
		rf.sendVoteRequestToPeers()
	// 如果是candidate超时, 那么term+1, 区分上一选举周期，重新开始选举
	case Candidate:
		rf.debug("timeout,retrying,logs=%v", rf.status.Logs)
		rf.status.CurrentTerm++
		rf.status.receiveVoteNum = 1
		rf.persist(nil)
		rf.electionTimer()
		rf.sendVoteRequestToPeers()
	// 如果是leader超时, 那么发送心跳
	case Leader:
		rf.debug("timeout,send heartbeat,logs=%v", rf.status.Logs)
		for i := range rf.peers {
			if i != rf.me {
				rf.leaderSendLogs(i)
			}
		}
		rf.resetLeaderTimer() // leader定期发送心跳
	}
}

func (rf *Raft) updateCommitIndex() {
	rf.status.matchIndex[rf.me] = rf.LastLogEntry().LogIndex
	sortedMatchIndex := append([]int(nil), rf.status.matchIndex...) // 简化复制操作
	sort.Ints(sortedMatchIndex)
	// 计算出中位数的索引N
	N := sortedMatchIndex[len(sortedMatchIndex)/2]
	rf.debug("update commitIndex,sortedMatchIndex=%v,N=%v", sortedMatchIndex, N)
	newCommitIdx, _ := rf.LogIndex2Idx("updateCommitIndex", N)

	//If there exists an N such that N > commitIndex, a majority
	//of matchIndex[i] ≥ N, and log[N].term == CurrentTerm:set commitIndex = N
	// todo why 确保新CommitIndex比当前的大，且日志条目属于当前任期（只有leader有权力commit当前任期的日志）
	if N > rf.status.commitIndex && rf.status.Logs[newCommitIdx].Term == rf.status.CurrentTerm {
		rf.applyCommittedLogs(N)
	}
}

// 负责将从oldCommitIndex到新的CommitIndex之间的日志条目应用到状态机
func (rf *Raft) applyCommittedLogs(newCommitIndex int) {
	// todo oldCommitIndex可能已经被快照覆盖了，发快照的时候能更新commitIndex?
	lastCommitIdx, ok1 := rf.LogIndex2Idx("[applyCommittedLogs] lastCommit", max(rf.status.commitIndex, rf.status.LastIncludedIndex))
	newCommitIdx, ok2 := rf.LogIndex2Idx("[applyCommittedLogs] newCommit", newCommitIndex)
	if !ok1 || !ok2 {
		rf.debug("lastCommitIndex=%v,newCommitIndex=%v,logs=%v", rf.status.commitIndex, newCommitIndex, rf.status.Logs)
		panic("LogIndex2Idx failed")
	}
	rf.debug("commit log, matchIndex=%v N=%v oldCommitIndex=%v lastCommitLog=%v, newCommitLog=%v,logs=%v",
		rf.status.matchIndex, newCommitIndex, rf.status.commitIndex, rf.status.Logs[lastCommitIdx], rf.status.Logs[newCommitIdx], rf.status.Logs)
	rf.status.commitIndex = newCommitIndex
	// todo 全改为数组下标时候，比较lastIncludeIndex和lastLogIndex会有问题吗
	for i := lastCommitIdx + 1; i <= newCommitIdx; i++ {
		rf.applyQueue.Enqueue(ApplyMsg{
			CommandValid: true,
			Command:      rf.status.Logs[i].Command,
			CommandIndex: rf.status.Logs[i].LogIndex,
		})
	}
}

func (rf *Raft) leaderSendLogs(server int) {
	if rf.status.state != Leader {
		rf.debug("not leader, but send logs to %v, check me", server)
	}
	if rf.status.nextIndex[server] <= rf.status.LastIncludedIndex {
		// 如果nextIndex小于等于lastIncludedIndex，说明需要发送快照
		rf.debug("nextIndex(%v) <= lastIncludedIndex(%v),send snapshot to %v", rf.status.nextIndex[server], rf.status.LastIncludedIndex, server)
		rf.sendSnapShotRequestToFollower(server)
	} else {
		// 否则发送日志
		// 切片引用防止函数中被并发修改，先复制
		logsCopy := rf.status.Logs.Copy()
		nextIndexCopy := make([]int, len(rf.status.nextIndex))
		copy(nextIndexCopy, rf.status.nextIndex)
		go func(server, term, commitIndex int, logs Logs, nextIndex []int) {
			//If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
			lastLogIndex := logs.LastLogEntry().LogIndex
			preLog, ok := logs.GetLogEntryByLogIndex(nextIndex[server] - 1)
			if !ok {
				rf.debug("get %v preLog failed, preLogIndex=%v,lastIncIndex=%v,logs=%v", server, nextIndex[server]-1, rf.status.LastIncludedIndex, logs)
				panic("get preLog failed")
			}
			// 需要发送新的日志
			if lastLogIndex >= nextIndex[server] {
				entries, ok := logs.GetLogEntriesBytesFromIndex(preLog.LogIndex + 1)
				if !ok {
					rf.debug("get logs failed, preLogIndex=%v,lastIncIndex=%v,logs=%v", nextIndex[server]-1, rf.status.LastIncludedIndex, logs)
					panic("get msgEntries failed")
				}
				go rf.sendAppendEntriesRequestToFollower(server, term, preLog, entries, commitIndex)
				rf.debug("sendAppendEntries to %v,preLogIndex=%v,rf.Logs(%v),msgEntries=%v,leaderCommitIndex=%v",
					server, preLog.LogIndex, logs, entries, commitIndex)
			} else {
				// If there are no new entries to send: send AppendEntries RPC with log entries starting at nextIndex - 1
				// 发送心跳
				rf.debug("send heartbeat to %v,nextIndex=%v,rf.Logs(%v)", server, nextIndex[server], logs)
				go rf.sendAppendEntriesRequestToFollower(server, term, preLog, nil, commitIndex)
			}
		}(server, rf.status.CurrentTerm, rf.status.commitIndex, logsCopy, nextIndexCopy)
	}
}

func (rf *Raft) applySnapshot(msg *installSnapshotRequest, newLogs []LogEntry) {
	//更新日志
	rf.status.Logs = newLogs
	rf.status.LastIncludedIndex = msg.LastIncludedIndex
	rf.status.LastIncludedTerm = msg.LastIncludedTerm
	rf.status.commitIndex = msg.LastIncludedIndex
	rf.persist(msg.Snapshot)
	rf.applyQueue.Clear()
	rf.applyQueue.Enqueue(ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      msg.Snapshot,
		SnapshotTerm:  msg.LastIncludedTerm,
		SnapshotIndex: msg.LastIncludedIndex,
	})
	go rf.sendInstallSnapshotReplyToLeader(msg, rf.status.CurrentTerm, true)
}

// 用来保存已经被使用过的任期号
func (rf *Raft) persist(snapShot []byte) {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	//创建一个 Gob 编码器，将编码结果写入 w
	e := labgob.NewEncoder(w)
	err := e.Encode(&rf.status.PersistInfo)
	if err != nil {
		panic(err)
	}
	raftstate := w.Bytes()
	if snapShot == nil {
		snapShot = rf.persister.ReadSnapshot()
	}
	rf.persister.Save(raftstate, snapShot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		rf.status.PersistInfo.Logs = make([]LogEntry, 1)
		rf.status.PersistInfo.CurrentTerm = 0
		rf.status.PersistInfo.VotedFor = -1
		return
	}
	//创建读取器r 和 解码器d
	r := bytes.NewReader(data)
	d := labgob.NewDecoder(r)
	if err := d.Decode(&rf.status.PersistInfo); err != nil {
		panic(err)
	}
}

func (rf *Raft) ProcessApplyQueue() {
	for {
		all := rf.applyQueue.DequeueAll()
		for _, v := range all {
			if Debug {
				log.Printf("[async thread] %v send to applyCh %v\n", rf.me, v)
			}
			msg := v.(ApplyMsg)
			if msg.SnapshotValid {
				if Debug {
					log.Printf("applyCh got snapshot: %s\n", string(msg.Snapshot))
				}
			}
			rf.applyChan <- msg
		}
	}
}

// 1.日志条目数量超过 SnapShotInterval
// 2.节点重新连接或恢复后需要同步快照
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	respChan := make(chan struct{})
	rf.snapShotChan <- SnapShotInfo{
		LastIncludedLogIndex: index,
		SnapShot:             snapshot,
		RespChan:             respChan,
	}
	<-respChan
}

func (rf *Raft) Kill() { atomic.StoreInt32(&rf.dead, 1) }

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:      peers,
		persister:  persister,
		me:         me,
		dead:       0,
		applyChan:  applyCh,
		applyQueue: NewUnboundedQueue(),
		status: Status{
			state:       Follower,
			commitIndex: 0,
			lastApplied: 0,
			LeaderInfo: LeaderInfo{
				nextIndex:  make([]int, len(peers)),
				matchIndex: make([]int, len(peers)),
			},
			CandidateInfo: CandidateInfo{
				receiveVoteNum: 0,
			},
			PersistInfo: PersistInfo{
				CurrentTerm:       0,
				VotedFor:          -1,
				Logs:              []LogEntry{{0, 0, nil}},
				LastIncludedIndex: 0,
				LastIncludedTerm:  0,
			},
		},
		messagePipeLine: make(chan Message),
		commandChan:     make(chan CommandInfo),
		snapShotChan:    make(chan SnapShotInfo),
	}
	// (first index is 1)
	for i := range rf.status.nextIndex {
		rf.status.nextIndex[i] = 1
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.Step()
	// 处理 applyQueue 中的所有 ApplyMsg 并发送到 applyCh
	go rf.ProcessApplyQueue()
	return rf
}

// 调用raft中的start函数，对leader节点写入log (然后检测log是否成功其实就是通过applyChan协程一直检测)
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	respChan := make(chan CommandRespInfo)
	rf.commandChan <- CommandInfo{
		Command:  command,
		RespChan: respChan,
	}
	resp := <-respChan
	return resp.LogIndex, resp.Term, resp.IsLeader
}
