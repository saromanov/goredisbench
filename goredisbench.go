package goredisbench

import
(
	"github.com/fzzy/radix/redis"
	"time"
	"fmt"
	"log"
)


type Goredisbench struct{
	client* redis.Client
	commands []string
}

func Init()*Goredisbench {
	grb := new(Goredisbench)
	clinet, err := redis.DialTimeout("tcp", "127.0.0.1:6379", time.Duration(10)*time.Second)
	if err != nil {
		panic(err)
	}
	grb.client = clinet
	return grb
}

func (grb*Goredisbench) AddCommands(commands []string) {
	if len(commands) > 0 {
		grb.commands = commands
	} else {
		log.Fatal("Number of commands must be greather then zero")
	}
}

func (grb*Goredisbench) Start(iters [] int){
	start := time.Now()
	for _, command := range grb.commands {
		comm := fmt.Sprintf("Command: %s", command)
		fmt.Println(comm)
		for _, it := range iters {
			for i := 0; i < it; i++ {
				item := fmt.Sprintf("%s%i%i", command, it, i)
				grb.client.Cmd("hmset", item, "fun")
			}
		}
	}
	end := time.Since(start)
	fmt.Println("Result: ", end.Seconds())

}