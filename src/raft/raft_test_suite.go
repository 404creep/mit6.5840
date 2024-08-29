package raft

import (
	"bytes"
	"fmt"
	"github.com/jedib0t/go-pretty/v6/table"
	progressbar "github.com/schollz/progressbar/v3"
	"log"
	"os"
	"sync"
	"testing"
	"time"
)

type TestCase struct {
	Name string
	Func func(*testing.T)
}

type TestResult struct {
	Name        string
	FailedCount int
	ElapsedTime time.Duration
}

func main() {
	testCases := []TestCase{
		{"TestInitialElection2A", TestInitialElection2A}, // 去掉了 raft.
		{"TestReElection2A", TestReElection2A},           // 去掉了 raft.
		{"TestManyElections2A", TestManyElections2A},
		{"TestBasicAgree2B", TestBasicAgree2B},
		{"TestRPCBytes2B", TestRPCBytes2B},
		{"TestFollowerFailure2B", TestFollowerFailure2B},
		{"TestLeaderFailure2B", TestLeaderFailure2B},
		{"TestFailAgree2B", TestFailAgree2B},
		{"TestFailNoAgree2B", TestFailNoAgree2B},
		{"TestConcurrentStarts2B", TestConcurrentStarts2B},
		{"TestRejoin2B", TestRejoin2B},
		{"TestBackup2B", TestBackup2B},
		{"TestCount2B", TestCount2B},
		{"TestPersist12C", TestPersist12C},
		{"TestPersist22C", TestPersist22C},
		{"TestPersist32C", TestPersist32C},
		{"TestFigure82C", TestFigure82C},
		{"TestUnreliableAgree2C", TestUnreliableAgree2C},
		{"TestFigure8Unreliable2C", TestFigure8Unreliable2C},
		{"TestReliableChurn2C", TestReliableChurn2C},
		{"TestUnreliableChurn2C", TestUnreliableChurn2C},
		{"TestSnapshotBasic2D", TestSnapshotBasic2D},
		{"TestSnapshotInstall2D", TestSnapshotInstall2D},
		{"TestSnapshotInstallUnreliable2D", TestSnapshotInstallUnreliable2D},
		{"TestSnapshotInstallCrash2D", TestSnapshotInstallCrash2D},
		{"TestSnapshotInstallUnCrash2D", TestSnapshotInstallUnCrash2D},
		{"TestSnapshotAllCrash2D", TestSnapshotAllCrash2D},
		{"TestSnapshotInit2D", TestSnapshotInit2D},
	}
	bar := progressbar.NewOptions(100*len(testCases), progressbar.OptionSetDescription("Running Tests"))

	var results []TestResult
	var mu sync.Mutex
	var wg sync.WaitGroup

	startTime := time.Now()

	for _, tc := range testCases {
		wg.Add(1)
		go func(tc TestCase) {
			defer wg.Done()

			failedCount := 0
			start := time.Now()

			for i := 0; i < 100; i++ {
				bar.Add(1) // 更新进度条

				var buf bytes.Buffer
				log.SetOutput(&buf) // 将 log 输出重定向到缓冲区
				t := &testing.T{}

				// 运行测试函数
				tc.Func(t)

				if t.Failed() {
					failedCount++
					saveLog(tc.Name, i+1, buf.String())
				}
			}

			elapsed := time.Since(start)

			mu.Lock()
			results = append(results, TestResult{
				Name:        tc.Name,
				FailedCount: failedCount,
				ElapsedTime: elapsed,
			})
			mu.Unlock()

		}(tc)
	}

	wg.Wait()

	totalTime := time.Since(startTime)
	printResults(results, totalTime)
}

func saveLog(testName string, iteration int, log string) {
	filename := fmt.Sprintf("%s_failed_iteration_%d.log", testName, iteration)
	f, err := os.Create(filename)
	if err != nil {
		fmt.Printf("Failed to create log file: %v\n", err)
		return
	}
	defer f.Close()

	_, err = f.WriteString(log)
	if err != nil {
		fmt.Printf("Failed to write to log file: %v\n", err)
	}
}

func printResults(results []TestResult, totalTime time.Duration) {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"Test Case", "Failed Count", "Elapsed Time"})

	for _, result := range results {
		t.AppendRow([]interface{}{result.Name, result.FailedCount, result.ElapsedTime})
	}

	t.Render()

	fmt.Printf("\nTotal Time: %v\n", totalTime)
}
