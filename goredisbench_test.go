package goredisbench

import
(
	"testing"
	"math/rand"
	"time"
)

func TestInit (t *testing.T) {
	fun := Init("127.0.0.1:6379")
	if fun == nil {
		t.Errorf("Error in Init function")
	}
}

func TestOneLoop (t *testing.T) {
	fun := Init("127.0.0.1:6379")
	fun.AddCommands([]string{"hset"})
	fun.Start([]int{10})
	status := fun.Status()
	if status[0] == 0 {
		t.Errorf("Error in TestOneLoop")
	}
	
}

func TestSeveralLoops (t *testing.T) {
	grb := Init("127.0.0.1:6379")
    grb.AddCommands([]string{"hset"})
    grb.Start([]int{1000,2000,10000})
}

func TestSeveralCommands (t *testing.T) {
	grb := Init("127.0.0.1:6379")
	grb.AddCommands([]string{"zadd", "zincrby"})
	grb.Start([]int{10})
	status := grb.Status()
	for _, st := range status {
		if st == 0 {
			t.Errorf("Error in TestSeveralCommands. One of the functions contain errors")
		}
	}
}


func TestDifferentCommands (t *testing.T) {
	grb := Init("127.0.0.1:6379")
	grb.AddCommands([]string{"zadd", "pfadd", "hset"})
	grb.Start([]int{10})
	status := grb.Status()
	for _, st := range status {
		if st == 0 {
			t.Errorf("Error in TestSeveralCommands. One of the functions contain errors")
		}
	}
}

func genf () string{
	rand.Seed(time.Now().UnixNano())
	result := make([]byte, 10)
	for i := 0; i < 10; i++ {
		result[i] = byte(65 + rand.Intn(90-65))
	}
	return string(result)
}


func TestUserFunc (t *testing.T) {
	grb := Init("127.0.0.1:6379")
	grb.AddCommands([]string{"hset"})
	grb.AddGeneration(genf)
	grb.Start([]int{1000})
}

func TestHyperloglogAdd (t *testing.T) {
	grb := Init("127.0.0.1:6379")
	grb.AddCommands([]string{"pfadd"})
	grb.Start([]int{10})
	status := grb.Status()
	if status[0] == 0{
		t.Errorf("Error in append with Hyperloglog")
	}
}

func TestHyperloglogCount (t *testing.T) {
	grb := Init("127.0.0.1:6379")
	grb.AddCommands([]string{"pfadd", "pfcount"})
	grb.Start([]int{10})
	status := grb.Status()
	if status[0] == 0{
		t.Errorf("Error in append with Hyperloglog")
	}
}

func TestHyperloglogMerge (t *testing.T) {
	grb := Init("127.0.0.1:6379")
	grb.AddCommands([]string{"pfadd", "pfmerge"})
	grb.Start([]int{10})
	status := grb.Status()
	if status[0] == 0{
		t.Errorf("Error in append with Hyperloglog")
	}
}

func TestCommandsTime (t *testing.T) {
	grb := Init("127.0.0.1:6379")
	result := grb.CommandsTime("zadd", time.Second * 2)
	if result == 0 {
		t.Errorf("Error in CommandsTime")
	}
}

