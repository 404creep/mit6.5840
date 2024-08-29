package raft

import (
	"6.5840/labgob"
	"bytes"
)

type LogEntry struct {
	Term     int
	LogIndex int
	Command  interface{}
}

func (logEntry *LogEntry) ToBytes() []byte {
	buf := bytes.NewBuffer(nil)
	en := labgob.NewEncoder(buf)
	en.Encode(*logEntry)
	return buf.Bytes()
}

type Logs []LogEntry

func BytesToLogEntry(bs []byte) (LogEntry, bool) {
	le := LogEntry{}
	de := labgob.NewDecoder(bytes.NewReader(bs))
	err := de.Decode(&le)
	return le, err == nil
}

// 用来区分日志下标和log里面的index
func (logs *Logs) LastLogEntry() LogEntry {
	return (*logs)[len(*logs)-1]
}

func (logs *Logs) LastIdx() int {
	return len(*logs) - 1
}

func (logs *Logs) GetLogEntryByLogIndex(logIndex int) (LogEntry, bool) {
	// 计算实际的切片索引
	idx := logIndex - (*logs)[0].LogIndex
	if idx < 0 || idx >= len(*logs) || (*logs)[idx].LogIndex != logIndex {
		return LogEntry{}, false
	}
	return (*logs)[idx], true
}
func (logs *Logs) GetLogEntriesBytesFromIndex(logIndex int) ([][]byte, bool) {
	// 计算实际的切片索引
	idx := logIndex - (*logs)[0].LogIndex
	if idx < 0 || idx >= len(*logs) {
		// 如果 logIndex 无效，返回空切片和 false
		return nil, false
	}
	var result [][]byte
	// 将从 idx 开始的所有日志条目转换为字节数组
	for _, logEntry := range (*logs)[idx:] {
		result = append(result, logEntry.ToBytes())
	}
	return result, true
}

// 裁剪日志，保留从 logIndex 对应的日志开始的部分
func (logs *Logs) TrimFrontLogs(logIndex int) {
	if len(*logs) == 0 {
		return
	}
	// 获取日志条目
	targetLog, _ := logs.GetLogEntryByLogIndex(logIndex)
	startIdx := targetLog.LogIndex - (*logs)[0].LogIndex

	// 错误处理：索引超出范围
	if startIdx < 0 || startIdx >= len(*logs) {
		return
	}

	// 裁剪日志切片
	*logs = (*logs)[startIdx:]

	// 将裁剪后第一个日志的 Command 设为 nil
	if len(*logs) > 0 {
		(*logs)[0].Command = nil
	}
}

// 裁剪日志，保留从开始到 logIndex 对应的日志的部分
func (logs *Logs) TrimBackLogs(logIndex int) {
	if len(*logs) == 0 {
		return
	}
	// 获取日志条目
	targetLog, ok := logs.GetLogEntryByLogIndex(logIndex)
	endIdx := targetLog.LogIndex - (*logs)[0].LogIndex
	// 错误处理：索引超出范围 或者 没有LogIndex的日志
	if endIdx < 0 || !ok {
		endIdx = 0
	}
	// 裁剪日志切片
	*logs = (*logs)[:endIdx]

}

func (logs *Logs) Copy() Logs {
	return append(Logs{}, *logs...)
}
