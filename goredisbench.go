package goredisbench

import (
	"fmt"
	"github.com/fzzy/radix/redis"
	"log"
	"math/rand"
	"time"
)

type (
	GENFUNC func() string
)

const (
	appendname = "foobar"
)

//Goredisbench creates
type Goredisbench struct {
	// Create client
	client   *redis.Client
	// Commands which contain current test
	commands []string
	//Function to generate data for tests
	genfunc  GENFUNC
	//Errors contains every test
	errors []float64
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
		globalstatus:= 0.0
		for _, it := range iters {
			comm := fmt.Sprintf("Number of iterations: %d", it)
			fmt.Println(comm)
			result,status := grb.loop(it, command, true)
			fmt.Println("Time to complete: ", result)
			//fmt.Println("Statusff: ", status)
			globalstatus = status
			grb.errors = append(grb.errors, status)
		}
		res := fmt.Sprintf("%f percents of operations is complete", globalstatus*100)
		fmt.Println(res)
		fmt.Println("\n")
	}
}


func (grb* Goredisbench) Status() []float64{
	return grb.errors
}
func (grb *Goredisbench) loop(it int, command string, showitmessage bool) (float64, float64) {
	start := time.Now()
	/*if showitmessage {
		itnumber := fmt.Sprintf("Number of iterations: %d", it)
		fmt.Println(itnumber)
	}*/
	allstatus := 0
	for i := 0; i < it; i++ {
		elem := command
		if grb.genfunc != nil {
			elem = grb.genfunc()
		}
		status := 0
		switch command {
		case "set":
			status = redis_set(grb.client, it, i)
		case "hset":
			status = redis_hset(grb.client, it, i)
		case "hget":
			status = redis_hsets_generic(grb.client, elem, it, i)
		case "hdel":
			status = redis_hsets_generic(grb.client, elem, it, i)
		case "hlen":
			status = redis_hlen(grb.client, it, i)
		case "lpush":
			status = redis_list_generic(grb.client, elem, it, i)
		case "rpush":
			status = redis_list_generic(grb.client, elem, it, i)
		case "zadd":
			status = redis_sset(grb.client, command, it, i)
		case "zrem":
			status = redis_sset_generic(grb.client, command, it, i)
		case "zrank":
			status = redis_sset_generic(grb.client, command, it, i)
		case "zincrby":
			status = redis_sset(grb.client, command, it, i)
		default:
			log.Fatal(fmt.Sprintf("Test for command %s not implemented yet", command))
		}
		if status > 0 {
			allstatus += 1
		}
	}
	end := time.Since(start)
	return end.Seconds(), float64(allstatus/it)
}

func (grb *Goredisbench) runWithAvg(iters []int, avgtimes int) {
	for _, command := range grb.commands {
		comm := fmt.Sprintf("Command: %s", command)
		fmt.Println(comm)
		globalstatus := 0.0
		for _, it := range iters {
			preresult := fmt.Sprintf("Average result after %d times of %d iterations", avgtimes, it)
			fmt.Println(preresult)
			avgvalue := 0.0
			for i := 0; i < avgtimes; i++ {
				result, status := grb.loop(it, command, false)
				globalstatus = status
				avgvalue += result
			}
			fmt.Println("Average time to complete: ", avgvalue/float64(avgtimes))
		}
		res := fmt.Sprintf("%f percents of operations is complete", globalstatus*100)
		fmt.Println(res)
		fmt.Println("\n")

	}
}

//Commands area

func redis_set(client *redis.Client, num1, num2 int) int {
	item := fmt.Sprintf("%s%d%d", appendname, num1, num2)
	result, _ := client.Cmd("set", item, "fun").Int()
	return result
}

/* Hashes */

func redis_hset(client *redis.Client, num1, num2 int) int {
	hashname := fmt.Sprintf("%s%d%d", appendname, num1, num2)
	field := fmt.Sprintf("%s%s%d%d", appendname, appendname, num1, num2)
	item := fmt.Sprintf("%d%s", num1*num2, appendname)
	result, _ := client.Cmd("hset", hashname, field, item).Int()
	return result
}

func redis_hlen(client *redis.Client, num1, num2 int) int {
	hashname := fmt.Sprintf("%s%d%d", appendname, num1, num2)
	result, _ := client.Cmd("hlen", hashname).Int()
	return result
}

func redis_hsets_generic(client *redis.Client, command string, num1, num2 int) int{
	hashname := fmt.Sprintf("%s%d%d", appendname, num1, num2)
	field := fmt.Sprintf("%s%s%d%d", appendname, appendname, num1, num2)
	result , _ := client.Cmd(command, hashname, field).Int()
	return result
}

/* Lists */

func redis_list_generic(client *redis.Client, command string, num1, num2 int) int{
	hashname := fmt.Sprintf("%s", appendname)
	field := fmt.Sprintf("%s%s%d%d", appendname, appendname, num1, num2)
	result, _ := client.Cmd(command, hashname, field).Int()
	return result
}

func redis_list_pop(client *redis.Client) int {
	hashname := fmt.Sprintf("%s", appendname)
	result, _ := client.Cmd("lpop", hashname).Int()
	return result
}

/* Sorted sets*/

//Additional overhead is random generation of rank
func redis_sset(client *redis.Client, command string, num1, num2 int) int{
	setname := fmt.Sprintf("%s%d%d", appendname, num1, num2)
	rand.Seed(time.Now().UnixNano())
	rank := rand.Intn(20)
	result , _:= client.Cmd(command, setname, rank, "fun").Int()
	return result
}

func redis_sset_generic(client *redis.Client, command string, num1, num2 int) int {
	setname := fmt.Sprintf("%s%d%d", appendname, num1, num2)
	result , _:= client.Cmd(command, setname, "fun").Int()
	return result
}

func redis_sset_interstore(client *redis.Client, command string, num1, num2 int) {

}
