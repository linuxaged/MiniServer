// Tracy Ma @ 2013.10
package main

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	MAX_CONN_NUM = 5
)

// 设置日志输出
func SetupLog() {
	if _, err0 := os.Stat("/var/log/ddserver.txt"); err0 == nil {
		logFile, err1 := os.Open("/var/log/ddserver.txt")
		if err1 != nil {
			panic("Error when open log file: " + err1.Error())
		}
		log.SetOutput(logFile)
	} else {
		f, err2 := os.Create("/var/log/ddserver.txt")
		if err2 != nil {
			panic("Error when create log file: " + err2.Error())
		}
		log.SetOutput(f)
	}

	// log.Println("# ddserver start at " + time.Now().Format("# 2006/01/02/15:04:05 #"))
	log.Println("#DDSERVER  START")
}

func LogOut(msg string) {
	// DEBUG
	fmt.Printf("#%s\n", msg)
	log.Printf("#%s\n", msg)
}

// 创建 redis 连接池
func CreateRedisConnPool() *redis.Pool {
	pool := &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", ":6379")
			if err != nil {
				LogOut("redis.Dial Error ")
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			if err != nil {
				LogOut("TestOnBorrow Error ")
			}
			return err
		},
	}
	return pool
}

// 解析和执行命令
func ParseAndExecCommand(connTcp net.Conn, connRedis redis.Conn) {
	defer connTcp.Close()
	defer connRedis.Close()

	LogOut("surve connnect from " + connTcp.RemoteAddr().String())

	// 考虑最长的命令 "zadd 最长五个字 10", 4 + 5 * 3 + 1 + 4 = 25
	buffer := make([]byte, 48)

	_, err := connTcp.Read(buffer)
	if err != nil {
		LogOut(err.Error())
	} else {
		LogOut("Receive cmd: " + "[" + string(buffer) + "]")
	}
	// 用 Field 好过 Split, 不受空格个数限制
	subStrings := strings.Fields(string(buffer))
	// DEBUG
	for i, s := range subStrings {
		fmt.Println("subStrings[%d]%s", i, s)
	}

	// http://stackoverflow.com/a/12377539/853569
	switch subStrings[0] {
	case "showAd":
		connTcp.Write([]byte("0"))
	case "zadd":
		// DEBUG
		fmt.Println(subStrings[1] + " end")
		fmt.Println(subStrings[2] + " end")
		fmt.Println(len(subStrings[2][:strings.Index(subStrings[2], "\x00")]))
		reply, err := connRedis.Do("zadd", "scoreboard", subStrings[1], subStrings[2][:strings.Index(subStrings[2], "\x00")])
		// reply, err := connRedis.Do("zadd", "scoreboard", subStrings[1], strings.TrimSpace(subStrings[2]))
		if err != nil {
			LogOut("Error when zadd: " + err.Error())
		} else {
			fmt.Printf("zadd scoreboard %s %s", subStrings[1], subStrings[2])
			if reply.(int64) == 1 {
				connTcp.Write([]byte("1"))
			} else {
				connTcp.Write([]byte("0"))
			}
		}
	case "zscore":
		score, err := connRedis.Do("zscore", "scoreboard", subStrings[1][:strings.Index(subStrings[1], "\x00")])
		if err != nil {
			LogOut("Error when zscore: " + err.Error())
		} else {
			if score == nil {
				LogOut("zscore: no such player [" + subStrings[1] + "]")
				connTcp.Write([]byte("")) // 如果不存在，返回空，客户端据此判断昵称占用
			} else {
				connTcp.Write(score.([]byte))
			}
		}
	case "zrevrank":
		rank, err := connRedis.Do("zrevrank", "scoreboard", subStrings[1][:strings.Index(subStrings[1], "\x00")])
		if err != nil {
			LogOut("Error when zrevrank: " + err.Error())
		} else {
			switch rank := rank.(type) {
			case int64:
				num := strconv.FormatInt(rank, 10) // int64 -> string
				fmt.Println("rank = " + num)
				connTcp.Write([]byte(num))
			}

		}
	// TODO: 当获取的玩家过多时，优化;
	case "zrevrange":
		reply, err := connRedis.Do("zrevrange", "scoreboard", subStrings[1], subStrings[2][:strings.Index(subStrings[2], "\x00")], "withscores")
		if err != nil {
			LogOut("zrevrange error: " + err.Error())
		} else {
			var result []byte
			switch reply := reply.(type) {
			case []interface{}:
				for i := range reply {
					if reply[i] == nil {
						continue
					}
					result = append(result, reply[i].([]byte)...)
					result = append(result, " "...)
				}
				connTcp.Write(result)
			case nil:
				connTcp.Write([]byte(""))
			}

		}
	}
}

func main() {

	SetupLog()

	// 创建 tcpListener
	tcpListener, err := net.ListenTCP("tcp4", &net.TCPAddr{
		IP:   net.IPv4(0, 0, 0, 0),
		Port: 2223,
	})
	if err != nil {
		LogOut(err.Error())
	}
	defer tcpListener.Close()

	// 创建 redis 连接池
	redisConnPool := CreateRedisConnPool()

	// 管道通讯
	var curTcpConnNum int = 0
	tcpConnChan := make(chan net.Conn)
	tcpConnNumChan := make(chan int)

	// 调整现在的链接数
	go func() {
		for numAdjust := range tcpConnNumChan {
			// 1, -1, 1, -1 ...
			curTcpConnNum += numAdjust
		}
	}()

	for i := 0; i < MAX_CONN_NUM; i++ {
		go func() {
			for conn := range tcpConnChan {
				tcpConnNumChan <- 1
				ParseAndExecCommand(conn, redisConnPool.Get())
				tcpConnNumChan <- -1
			}
		}()
	}

	for {
		conn, err := tcpListener.Accept()
		if err != nil {
			LogOut(err.Error())
			return
		}
		tcpConnChan <- conn // 有链接进来了
	}

}
