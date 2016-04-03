package mock

import (
	_ "fmt"
	. "github.com/cs733-iitb/cluster"
	"sync"
	"testing"
	"time"
)

const NUMSERVERS = 5

type MockMsg struct {
	Id  int
	Msg string
}

// create servers with pids from 1..NUMSERVERS inclusive
func mkCluster() (*MockCluster, error) {
	config := Config{Peers:[]PeerConfig{
		{Id:1}, {Id:2}, {Id:3}, {Id:4}, {Id:5},
	}}
	cl, err := NewCluster(config)
	return cl, err 
}

func broadcastRcv(cl *MockCluster, numSenders int, numMsgs int, waitForMs int, dropProbability float32, maxDelayMs int) (numUnique, numRcvd int) {
	maxWait := time.Duration(waitForMs) * time.Millisecond

	var mutex sync.Mutex // for rcvd and totalrcvd
	// track unique msgids received.
	rcvd := make(map[int]bool, numMsgs*numSenders)
	// total number of received messages. If no loss, it should be numMsgs * numSenders * (NUMSERVERS - 1)
	// because each server has (NUMSERVERS-1) peers
	totalRcvd := 0

	var wg sync.WaitGroup

	if waitForMs <= maxDelayMs+10 {
		panic("Error test: wait a little more than maxDelayMs")
	}

	pumper := func(senderPid int) {
		defer wg.Done()
		for i := 0; i < numMsgs; i++ {
			msgid := int64(i)<<NUMSERVERS | int64(senderPid)
			cl.Servers[senderPid].Outbox() <- &Envelope{Pid: BROADCAST, MsgId: msgid, Msg: "hello"}
			if i%100 == 0 {
				time.Sleep(1 * time.Millisecond)
			}
		}
	}

	drainer := func(rcvrPid int) {
		defer wg.Done()
		for {
			select {
			case e := <-cl.Servers[rcvrPid].Inbox():
				mutex.Lock()
				totalRcvd += 1
				rcvd[int(e.MsgId)] = true
				mutex.Unlock()
			case <-time.After(maxWait):
				return
			}
		}
	}

	for i := 1; i <= NUMSERVERS; i++ {
		wg.Add(1)
		go drainer(i)
	}

	time.Sleep(5 * time.Millisecond)
	for i := 1; i <= numSenders; i++ {
		wg.Add(1)
		go pumper(i)
	}
	wg.Wait()
	return len(rcvd), totalRcvd
}

func TestCreate(t *testing.T) {
	cl, err := mkCluster()
	defer cl.Close()
	if err != nil {
		t.Error(err)
	}
	defer cl.Close()

	// ensure that each server knows about the other.
	for i := 1; i < NUMSERVERS; i++ {
		srv, ok := cl.Servers[i]
		if ok {
			// Check srv.peers has unique members, and has the correct length, and doesn't include itself
			peerIds := make(map[int]int, NUMSERVERS)
			for _, peerId := range srv.Peers() {
				if _, peerOk := cl.Servers[peerId]; !peerOk {
					t.Errorf("Peer %d not found for server %d", peerId, srv.Pid())
				}
				peerIds[peerId] = 1
				if peerId == srv.Pid() {
					t.Error("Server %d has self as peer")
				}
			}
			if len(peerIds) != NUMSERVERS - 1 {
				t.Error("Server %d only has %d peers. Expected: %d", srv.Pid(), len(peerIds), NUMSERVERS-1)
			}
		} else {
			t.Error("Server not found")
		}
	}

	
}

// Broadcast from server 0, expect to receive on all others
func TestNoloss(t *testing.T) {
	cl, _ := mkCluster()
	defer cl.Close()

	nSenders := 1
	nMsgs := 100
	nExpected := nSenders * (NUMSERVERS - 1) * nMsgs // total expected
	nUniq, nRcvd := broadcastRcv(cl, nSenders, nMsgs /*waitMs*/, 200 /*dropProb*/, 0 /*send delay*/, 0)

	if nUniq != nMsgs {
		t.Errorf("Sent (and expected to receive): %d. Received: %d", nMsgs, nUniq)
	}
	if nExpected != nRcvd {
		t.Errorf("Sent : %d. Total expected : %d. Received: %d", nMsgs, nExpected, nRcvd)
	}

}

func TestSendOmission(t *testing.T) {
	cl, _ := mkCluster()
	defer cl.Close()

	var dropProb float32 = 0.30

	// Tacitly assuming nSenders == 1
	cl.Servers[1].SetDropProbability(dropProb)

	nMsgs := 10000
	nMinExpected := int(float32(nMsgs) * (1.0 - dropProb)) // Better receive more than this

	_, nRcvd := broadcastRcv(cl /*nSenders*/, 1, nMsgs /*waitMs*/, 200 /*drop Prob*/, dropProb /*send delay*/, 0)

	if nRcvd == (NUMSERVERS-1)*nMsgs {
		t.Error("Should have dropped some messages")
	} else if nRcvd < nMinExpected {
		t.Errorf("Too many msgs dropped. Sent = %d. Expected To Receive At Least = %d, Rcvd = %d (%f%% of sent)",
			nMsgs, nMinExpected, nRcvd, float32(nRcvd)*100.0/float32(nMsgs))
	}
}

func TestOutOfOrder(t *testing.T) {
	cl, _ := mkCluster()
	defer cl.Close()

	nSenders := 1
	nMsgs := 100
	nExpected := nSenders * (NUMSERVERS - 1) * nMsgs
	nUniq, nRcvd := broadcastRcv(cl /*nSenders*/, nSenders, nMsgs /*waitMs*/, 500 /*drop Prob*/, 0 /*send delay*/, 100)
	if nUniq != nMsgs {
		t.Errorf("Sent (and expected to receive): %d. Received: %d", nMsgs, nUniq)
	}
	if nExpected != nRcvd {
		t.Errorf("Sent : %d. Total expected : %d. Received: %d", nMsgs, nExpected, nRcvd)
	}
}

func TestPartition(t *testing.T) {
	cl, _ := mkCluster()
	defer cl.Close()

	nMsgs := 100

	nSenders := 5
	cl.Partition([]int{1, 2, 3}, []int{4, 5})
	// for a partition with p servers, and with each broadcasting to p-1 peers
	//   nummsgs expected to be received in that partition = nMsgs * p * (p-1)
	nExpected := nMsgs * (3*2 + 2*1)
	_, nRcvd := broadcastRcv(cl /*nSenders*/, nSenders, nMsgs /*waitMs*/, 100 /*drop Prob*/, 0 /*send delay*/, 0)

	if nExpected != nRcvd {
		t.Errorf("Sent : %d. Total expected : %d. Received: %d", nMsgs, nExpected, nRcvd)
	}

	cl.Heal()

	nExpected = nMsgs * NUMSERVERS * (NUMSERVERS - 1)

	_, nRcvd = broadcastRcv(cl /*nSenders*/, nSenders, nMsgs /*waitMs*/, 100 /*drop Prob*/, 0 /*send delay*/, 0)

	if nExpected != nRcvd {
		t.Errorf("Sent : %d. Total expected : %d. Received: %d", nMsgs, nExpected, nRcvd)
	}

}

func TestShutdownAndRestart(t *testing.T) {
	cl, _ := mkCluster()
	defer cl.Close()

	cl.Servers[1].Close()
	_, err := cl.AddServer(1)
	if err != nil {
		t.Fatal(err)
	}
}
