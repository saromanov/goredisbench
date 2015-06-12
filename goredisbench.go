package goredisbench

import (
	"fmt"
	"github.com/fzzy/radix/redis"
	"log"
	"math/rand"
	"time"
	"strings"
	"sync/atomic"
	"runtime"
)

type (
	GENFUNC func() string
)

const (
	appendname     = "foobar1"
	appendhashname = "hash" + appendname
	appendsetname  = "set" + appendname
	appendhllname  = "hll" + appendname
	appendlistname = "list" + appendname
)

//Goredisbench creates
type Goredisbench struct {
	// Create client
	client *redis.Client
	// Commands which contain current test
	commands []string
	//Function to generate data for tests
	genfunc GENFUNC
	//Errors contains every test
	errors []float64

	showerrormessages bool
}

type Options struct {
	//Show average time of running command after avgtimes times
	Avgtimes int

	//This option allows to show error messages during tests
	Showerrors bool
}

//Init creates and returns object of Goredisbench structure
func Init(addr string) *Goredisbench {
	grb := new(Goredisbench)
	clinet, err := redis.DialTimeout("tcp", addr, time.Duration(10)*time.Second)
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

	grb.client.Cmd("del", appendhllname)
	grb.client.Cmd("del", appendname)
	grb.client.Cmd("del", appendhashname)
	grb.client.Cmd("del", appendsetname)
	grb.client.Cmd("del", appendlistname)
	fmt.Println(redis_info(grb.client, "redis_version"))

	if len(opt) > 0 && opt[0].Showerrors {
		grb.showerrormessages = opt[0].Showerrors
	}
	if len(opt) > 0 && opt[0].Avgtimes > 0 {
		grb.runWithAvg(iters, opt[0].Avgtimes)
	} else {
		grb.run(iters)
	}
}

//CommandsTime returns number of commands executed per time num
func (grb *Goredisbench) CommandsTime(command string, numtime time.Duration)uint64 {
	var ops uint64 = 0
    go func() {
        for {
        	atomic.AddUint64(&ops, 1)
        	redis_list_generic(grb.client, command, "A", false)
        	runtime.Gosched()
        }
    }()
    time.Sleep(numtime)
    return atomic.LoadUint64(&ops)
}

func (grb *Goredisbench) run(iters []int) {
	for _, command := range grb.commands {
		comm := fmt.Sprintf("Command: %s", command)
		if command == "lpushx" || command == "rpushx" {
			redis_list_generic(grb.client, "lpush", "A", false)
		}
		fmt.Println(comm)
		globalstatus := 1.0
		for _, it := range iters {
			comm := fmt.Sprintf("Number of iterations: %d", it)
			fmt.Println(comm)
			result, status := grb.loop(it, command, grb.showerrormessages)
			fmt.Println("Time to complete: ", result)
			globalstatus = status
			grb.errors = append(grb.errors, float64(status))
		}
		res := fmt.Sprintf("%f percents of operations is complete", globalstatus*100)
		fmt.Println(res)
		fmt.Println("\n")
	}
}

func (grb *Goredisbench) Status() []float64 {
	return grb.errors
}

func (grb *Goredisbench) loop(it int, command string, showerrormessages bool) (float64, float64) {
	start := time.Now()
	allstatus := 0.0
	for i := 0; i < it; i++ {
		elem := fmt.Sprintf("%s%d%d", appendname, it, i)
		if grb.genfunc != nil {
			elem = grb.genfunc()
		}
		status := 0
		switch command {
		case "set":
			status = redis_set(grb.client, it, i, showerrormessages)
		case "hset":
			status = redis_hset(grb.client, elem, showerrormessages)
		case "hget":
			status = redis_hsets_generic(grb.client, command, elem, showerrormessages)
		case "hdel":
			status = redis_hsets_generic(grb.client, command, elem, showerrormessages)
		case "hlen":
			status = redis_hlen(grb.client, showerrormessages)
		case "lpush":
			status = redis_list_generic(grb.client, command, elem, showerrormessages)
		case "lpushx":
			status = redis_list_generic(grb.client, command, elem, showerrormessages)
		case "rpushx":
			status = redis_list_generic(grb.client, command, elem, showerrormessages)
		case "rpush":
			status = redis_list_generic(grb.client, command, elem, showerrormessages)
		case "zadd":
			status = redis_sset(grb.client, command, elem, showerrormessages)
		case "zrem":
			status = redis_sset_generic(grb.client, command, elem, showerrormessages)
		case "zrank":
			status = redis_sset_generic(grb.client, command, elem, showerrormessages)
		case "zincrby":
			status = redis_sset(grb.client, command, elem, showerrormessages)
		case "pfadd":
			status = redis_hyperloglog(grb.client, elem, showerrormessages)
		case "pfcount":
			status = redis_hyperloglog_count(grb.client, showerrormessages)
		case "pfmerge":
			status = redis_hyperloglog_merge(grb.client, elem, elem+elem, showerrormessages)
		default:
			log.Fatal(fmt.Sprintf("Test for command %s not implemented yet", command))
		}
		if status > 0 {
			allstatus += 1
		}
	}
	end := time.Since(start)
	return end.Seconds(), allstatus / float64(it)
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

func checker(err error, isshow bool) {
	if err != nil && isshow {
		log.Fatal(err)
	}
}

func redis_set(client *redis.Client, num1, num2 int, msg bool) int {
	item := fmt.Sprintf("%s%d%d", appendname, num1, num2)
	result, err := client.Cmd("set", item, "fun").Int()
	checker(err, msg)
	return result
}

/* Hashes */

func redis_hset(client *redis.Client, setname string, msg bool) int {
	result, err := client.Cmd("hset", appendhashname, setname, setname).Int()
	if err != nil && msg {
		fmt.Println(err)
	}
	return result
}

func redis_hlen(client *redis.Client, msg bool) int {
	result, _ := client.Cmd("hlen", appendlistname).Int()
	return result
}

func redis_hsets_generic(client *redis.Client, command string, setname string, msg bool) int {
	result, err := client.Cmd(command, appendhashname, setname).Int()
	checker(err, msg)
	return result
}

/* Lists */

func redis_list_generic(client *redis.Client, command string, setname string, msg bool) int {
	result, err := client.Cmd(command, appendlistname, setname).Int()
	checker(err, msg)
	return result
}

func redis_list_pop(client *redis.Client, msg bool) int {
	result, err := client.Cmd("lpop", appendlistname).Int()
	checker(err, msg)
	return result
}

/* Sorted sets*/

//Additional overhead is random generation of rank
func redis_sset(client *redis.Client, command string, setname string, msg bool) int {
	rand.Seed(time.Now().UnixNano())
	rank := rand.Intn(20)
	result, err := client.Cmd(command, appendsetname, rank, setname).Int()
	checker(err, msg)
	return result
}

func redis_sset_generic(client *redis.Client, command string, setname string, msg bool) int {
	result, err := client.Cmd(command, appendsetname, setname).Int()
	checker(err, msg)
	return result
}

func redis_sset_interstore(client *redis.Client, command string, num1, num2 int, msg bool) {

}

/* Hyperloglog */

func redis_hyperloglog(client *redis.Client, hashname string, msg bool) int {
	//client.Cmd("hrem", "fun")
	result, err := client.Cmd("pfadd", appendhllname, hashname).Int()
	checker(err, msg)
	return result
}

func redis_hyperloglog_count(client *redis.Client, msg bool) int {
	result, err := client.Cmd("pfcount", appendhllname).Int()
	checker(err, msg)
	return result
}

//Merge two sets
func redis_hyperloglog_merge(client *redis.Client, hashname1, hashname2 string, msg bool) int {
	redis_hyperloglog(client, hashname1, msg)
	redis_hyperloglog(client, hashname2, msg)
	result, err := client.Cmd("pfmerge", appendhllname, hashname1, hashname2).Int()
	checker(err, msg)
	return result
}


/* Info */

//Get information from redis from command INFO
func redis_info(client *redis.Client, param string)string {
	lines := strings.Split(client.Cmd("info").String(), "\n")
    for _, line := range lines {
    	if strings.HasPrefix(line ,param) {
    		return line
    	}
    }
    return ""
}
