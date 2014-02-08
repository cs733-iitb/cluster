package cluster

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

const numMsgs = 5000

type Stats struct {
	server      Server
	numMsgsRcvd int
	err         error
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


func TestBroadcast(t *testing.T) {
	stats, err := prepServer(1)
	if err != nil {
		t.Error(err)
		return
	}
	numServers := len(stats.server.Peers()) + 1
	numExpectedMsgs := numMsgs * (numServers-1)
	clusterstats := make ([]*Stats, numServers)
	clusterstats[0] = stats
	if err != nil {
		t.Error(err) 
		return
	}
	// get num peers
	for i := 1; i < numServers; i++ {
		stats, err := prepServer(i+1)
		if err != nil {
			t.Error(err)
			return
		}
		clusterstats[i] = stats
	}
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
				expectedMsg := fmt.Sprintf("Msg:%d", env.MsgId)
				if env.Msg != expectedMsg {
					stats.err = errors.New(fmt.Sprintf("Expected msg:'%s', got: '%s'", expectedMsg, env.Msg))
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
			env := &Envelope{Pid: BROADCAST, MsgId: int64(i), Msg: fmt.Sprintf("Msg:%d", i)}
			stats.server.Outbox() <- env
			if i%10 == 0 {
			      time.Sleep(1 * time.Millisecond)
			}
		}
	}
	wg.Add(numServers * 2)
	for i := 0; i < numServers; i++ {
		go drainer(clusterstats[i])
	}
	time.Sleep(1 * time.Second)
	for i := 0; i < numServers; i++ {
		go pumper(clusterstats[i])
	}
	wg.Wait()
	for i := 0; i < numServers; i++ {
		clusterstats[i].check(t, numServers)
	}
}
