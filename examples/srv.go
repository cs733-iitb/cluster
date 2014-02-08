package main

import (
	"github.com/sriram-srinivasan/cluster"
	"flag"
	"fmt"
	"os"
	"bufio"
	"strings"
	"strconv"
)
func main() {
	flag.Parse()
	myid, _ := strconv.Atoi(flag.Args()[0])
	server, err := cluster.New(myid, "cluster_test_config.json")

	if err != nil {
		panic(err)
	}
	go func() {
		for {
			env := <- server.Inbox()
			fmt.Printf("[From: %d MsgId:%d] %s\n", env.Pid, env.MsgId, env.Msg)
		}
	}()
	var msgid int64
	for {
		str := readLine() 
		server.Outbox() <- &cluster.Envelope{Pid: cluster.BROADCAST, MsgId: msgid, Msg: str}
		msgid = msgid + 1
	}
}

func readLine() string {
        bio := bufio.NewReader(os.Stdin)
        line, err := bio.ReadString('\n')
        if (err == nil && line != "") {
                line = strings.TrimSpace(line)
        }
        return line
}
