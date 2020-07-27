package raft

import (
	"log"
	"time"
	"math/rand"
)
// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func RandTimeout(mini int, maxi int) int {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return mini + r.Intn(maxi - mini)
}

func Max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func Min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}
