# GoRedisbench
Create benchmarks for redis
# Install
```sh
go get github.com/saromanov/goredisbench
```
 
# Usage
```go
//Init client
fun := goredisbench.Init("127.0.0.1:6379")
//Create benchmarks for commands ZADD and ZREM
fun.AddCommands([]string{"zadd", "zrem"})
//Start for 1000 iterations
fun.Start([]int{1000})
```
Also, you can make benchmark cases for several number of iterations
```go
fun.Start([]int{1000,5000,10000})
```

Output will looks like
```sh 
Command: zadd
Number of iterations: 1000
Time to complete:  0.238416287
100.000000 percents of operations is complete


Command: zrem
Number of iterations: 1000
Time to complete:  0.158961706
100.000000 percents of operations is complete
```

# Supported commands
ZADD, ZREM, ZRANK, ZINCRBY, SET, HSET, HGET, HDEL, HLEN, LPUSH, RPUSH, LPUSHX, RPUSHX, PFADD, PFCOUNT, PFMERGE

# LICENCE
MIT
