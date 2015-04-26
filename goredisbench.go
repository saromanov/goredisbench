package goredisbench

import (
	"fmt"
	"github.com/fzzy/radix/redis"
	"log"
	"time"
)

type (
	GENFUNC func() string
)

//Goredisbench creates
type Goredisbench struct {
	client   *redis.Client
	commands []string
	genfunc  GENFUNC
}

type Options struct {
	//Show average time of running command after avgtimes times
	Avgtimes int
}

//Init creates and returns object of Goredisbench structure
func Init() *Goredisbench {
	grb := new(Goredisbench)
	clinet, err := redis.DialTimeout("tcp", "127.0.0.1:6379", time.Duration(10)*time.Second)
	if err != nil {
		panic(err)
	}
	grb.client = clinet
	return grb
}

//AddCommands is append command(for example hmset) to benchmarking
func (grb *Goredisbench) AddCommands(commands []string) {
	if len(commands) > 0 {
		grb.commands = commands
	} else {
		log.Fatal("Number of commands must be greather then zero")
	}
}

//AddGeneration provides user generation of data for inserting to redis
func (grb *Goredisbench) AddGeneration(fun GENFUNC) {
	grb.genfunc = fun
}

func (grb *Goredisbench) Start(iters []int, opt ...Options) {
	if len(opt) > 0 && opt[0].Avgtimes > 0 {
		grb.runWithAvg(iters, opt[0].Avgtimes)
	} else {
		grb.run(iters)
	}
}

func (grb *Goredisbench) run(iters []int) {
	for _, command := range grb.commands {
		comm := fmt.Sprintf("Command: %s", command)
		fmt.Println(comm)
		for _, it := range iters {
			comm := fmt.Sprintf("Number of iterations: %d", it)
			fmt.Println(comm)
			result := grb.loop(it, command, true)
			fmt.Println("Result: ", result)
		}
	}
}

func (grb *Goredisbench) loop(it int, command string, showitmessage bool) float64 {
	start := time.Now()
	if showitmessage {
		itnumber := fmt.Sprintf("Number of iterations: %d", it)
		fmt.Println(itnumber)
	}
	for i := 0; i < it; i++ {
		if grb.genfunc == nil {
			item := fmt.Sprintf("%s%d%d", command, it, i)
			grb.client.Cmd("hmset", item, "fun")
		} else {
			grb.client.Cmd(command, grb.genfunc(), "fun")
		}
	}
	end := time.Since(start)
	return end.Seconds()
}

func (grb *Goredisbench) runWithAvg(iters []int, avgtimes int) {
	for _, command := range grb.commands {
		comm := fmt.Sprintf("Command: %s", command)
		fmt.Println(comm)
		for _, it := range iters {
			preresult := fmt.Sprintf("Average result after %d times of %d iterations", avgtimes, it)
			fmt.Println(preresult)
			avgvalue := 0.0
			for i := 0; i < avgtimes; i++ {
				result := grb.loop(it, command, false)
				avgvalue += result
			}
			fmt.Println("Result: ", avgvalue/float64(avgtimes))
		}

	}
}
