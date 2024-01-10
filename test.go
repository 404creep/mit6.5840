package main

import (
	"fmt"
	"time"
)

// 返回一个“返回int的函数”
func fibonacci() func() int {
	a := 0
	b := 1
	return func() int {
		res := a
		a = b
		b = a + b
		return res
	}

}

func main() {
	//go func() {
	//	for i := 0; i < 10; i = i + 1 {
	//		fmt.Println(i)
	//	}
	//}()
	for i := 0; i < 10; i++ {
		go func(val int) {
			fmt.Println(val)
		}(i)
	}
	fmt.Println("Finished")
	time.Sleep(time.Second * 2)
}
