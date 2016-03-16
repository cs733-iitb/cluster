package cluster

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
	"encoding/gob"
)

const numMsgs = 1000

type Stats struct {
	server      Server
	numMsgsRcvd int
	err         error
}

type MyMsg struct {
	MyMsgId int64
	Str     string
}

func init() {
	gob.Register(MyMsg{})
}

func (stats *Stats) check(t *testing.T, numServers int) {
	if stats.err != nil {
		t.Error(stats.err)
	}
	numMsgsExpected := numMsgs * (numServers - 1)
	if stats.numMsgsRcvd != numMsgsExpected  {
		t.Errorf("Srvr %d: Expected to receive %d msgs, got %d\n", stats.server.Pid(),
			numMsgsExpected, stats.numMsgsRcvd)
	}
}

func prepServer(id int) (stats *Stats, err error) {
	stats = new(Stats)
	srv, err := New(id, "cluster_test_config.json")
	stats.server = srv
	return
}


func TestBegin(t *testing.T) {
	New(100, Config{
		Peers: []PeerConfig{
			{Id: 100, Address: "localhost:7070"},
			{Id: 200, Address: "localhost:8080"},
			{Id: 300, Address: "localhost:9090"},
		},
	})
}

func prepCluster() (clusterStats []*Stats, err error) {
	stats, err := prepServer(1)
	if err != nil {
		return nil, err
	}
	numServers := len(stats.server.Peers()) + 1

	clusterstats := make ([]*Stats, numServers)
	clusterstats[0] = stats
	// get num peers
	for i := 1; i < numServers; i++ {
		stats, er := prepServer(i+1)
		if er != nil {
			return nil, er
		}
		clusterstats[i] = stats
	}
	return clusterstats, nil
}

func closeCluster(clusterStats []*Stats) {
	for _, stats := range clusterStats {
		stats.server.Close()
	}
}

func TestBroadcast(t *testing.T) {
	clusterStats, err := prepCluster()
	numServers := len(clusterStats)
	numExpectedMsgs := numMsgs * (numServers-1)
	if err != nil {
		t.Error(err)
	}
	defer closeCluster(clusterStats)

	var wg sync.WaitGroup
	drainer := func(stats *Stats) {
		defer wg.Done()
		srv := stats.server
		for {
			select {
			case env := <-srv.Inbox():
				if env.MsgId == 0 {
					stats.err = errors.New("MsgId == 0")
					return
				}
				stats.numMsgsRcvd += 1
				if env.Pid == srv.Pid() {
					stats.err = errors.New("Rcvd msg from self")
					return
				}
				expectedMsgStr := fmt.Sprintf("Msg:%d", env.MsgId)
				m := env.Msg.(MyMsg)
				if m.Str != expectedMsgStr {
					stats.err = errors.New(fmt.Sprintf("Expected msg:'%s', got: '%s'", expectedMsgStr, m.Str));
					return
				}
				if stats.numMsgsRcvd == numExpectedMsgs {
					return
				}

			case <-time.After(10 * time.Second):
				stats.err = errors.New("TIMED OUT")
				return
			}
		}
	}
	pumper := func(stats *Stats) {
		defer wg.Done()
		for i := 1; i <= numMsgs; i++ {
			msg := &MyMsg{MyMsgId : int64(i), Str:  fmt.Sprintf("Msg:%d", i)}
			env := &Envelope{Pid: BROADCAST, MsgId: int64(i), Msg: msg}
			stats.server.Outbox() <- env
			if i%10 == 0 {
			      time.Sleep(1 * time.Millisecond)
			}
		}
	}
	wg.Add(numServers * 2)
	for i := 0; i < numServers; i++ {
		go drainer(clusterStats[i])
	}
	time.Sleep(1 * time.Second)
	for i := 0; i < numServers; i++ {
		go pumper(clusterStats[i])
	}
	wg.Wait()
	for i := 0; i < numServers; i++ {
		clusterStats[i].check(t, numServers)
	}
}


func sendRcv(t *testing.T) {
	clusterStats, err := prepCluster()
	if err != nil {
		t.Fatal(err)
	}
	defer closeCluster(clusterStats)

	msg := &MyMsg{MyMsgId : 1, Str:  "sendRcv"}
	env := &Envelope{Pid: clusterStats[1].server.Pid(), Msg: msg}
	clusterStats[0].server.Outbox() <- env
	select {
	case <- clusterStats[1].server.Inbox(): return
	case <- time.After(500 * time.Millisecond):
		t.Fatal("Message not delivered")
	}
}


func TestCloseAndRestart(t *testing.T) {
	sendRcv(t)
	sendRcv(t)
}
