package mock

import (
	"errors"
	"fmt"
	. "github.com/cs733-iitb/cluster"
	"math/rand"
	"sync"
	"time"
)

var QUIT *Envelope

func init() {
	QUIT = &Envelope{} // Special marker to use in Close()
}
type MockCluster struct {
	sync.Mutex
	Servers map[int]*MockServer
}

type MockServer struct {
	sync.Mutex

	// peer id
	pid int

	// Cluster the server belongs to
	mc *MockCluster

	// peers in current partition
	peers map[int]*MockServer

	// all configured peers
	allpeers map[int]*MockServer

	// Probability (0.0 to 1.0) that a message will be dropped upon send. Default 0
	// This is only for messages to peers in current partition
	dropProbability float32

	// If non-zero, messages may be arbitrarily delayed and delivered.
	// It is subject to the current partition and dropProbability.
	maxMessageDelayMs int

	// For outgoing messages (client->server)
	outbox chan *Envelope

	// For incoming messages (server->client)
	inbox chan *Envelope

	// server-server messaging
	clusterbox chan *Envelope

	closed bool
}

// NewCluster accepts the same configuration as cluster.New. All it needs are the Ids
// of the Peers, not the addresses. Alternatively, a nil configuration can be supplied
// AddServer() called instead.
// 
func NewCluster(config interface{}) (*MockCluster, error) {
	cl := MockCluster{Servers: make(map[int]*MockServer)}
	if config != nil {
		cfg, err := ToConfig(config)
		if err != nil {
			return nil, err
		}
		for _, peer := range cfg.Peers {
			cl.AddServer(peer.Id)
		}
	}
	return &cl, nil
}

func (mc *MockCluster) AddServer(pid int) (Server, error) {
	mc.Lock()
	defer mc.Unlock()

	if _, ok := mc.Servers[pid]; ok {
		// duplicates not allowed
		return nil, errors.New("Duplicate pid")
	}

	ms := mc.newMockServer(pid)
	for _, srv := range mc.Servers {
		// mutual introductions
		srv.AddPeer(ms)
		ms.AddPeer(srv)
	}
	mc.Servers[ms.pid] = ms
	return ms, nil
}


func (mc *MockCluster) newMockServer(pid int) *MockServer {
	var ms MockServer
	ms.mc = mc // The cluster it belongs to
	ms.pid = pid
	ms.peers = make(map[int]*MockServer)
	ms.allpeers = make(map[int]*MockServer)
	ms.inbox = make(chan *Envelope, 100)
	ms.outbox = make(chan *Envelope, 100)
	ms.clusterbox = make(chan *Envelope, 100)
	go ms.receiveForever()
	go ms.sendForever()
	return &ms
}

// removeServer is not public. Call Server.Close() to remove
// it from the cluster.
func (mc *MockCluster) removeServer(pid int) {
	mc.Lock()
	defer mc.Unlock()
	delete(mc.Servers, pid)
}


// Convenience function to invoke a function on all configured servers.
func (mc *MockCluster) ForAll(do func(*MockServer)) {
	mc.Lock()
	for _, srv := range mc.Servers {
		do(srv)
	}
	mc.Unlock()
}

func (mc *MockCluster) Close() {
	mc.ForAll(func(srv *MockServer) { srv.doClose(/*cluster close=*/true) })
	mc.Servers = make(map[int]*MockServer)
}

// Group servers by partition. Those not mentioned explicitly are lumped together in a default partition
// A server may only belong to one partition at a time
func (mc *MockCluster) Partition(partitions ...[]int) error {
	index := make(map[int]int)         // Pid -> index of partitions array
	for ip, part := range partitions { // for each partition
		for _, pid := range part { // for each pid in that partition
			// Associate pid to partition if not already assigned
			if partid, ok := index[pid]; ok {
				if partid != ip {
					return errors.New(fmt.Sprintf("Server id %d in different partitions: %+v", partid, partitions))
				}
			} else {
				index[pid] = ip // Add server to partition ip
			}
		}
	}

	// Create default partition for pids not accounted for.
	defaultPartition := []int{}
	idefaultPartition := len(partitions)
	for pid, _ := range mc.Servers {
		if _, ok := index[pid]; !ok {
			defaultPartition = append(defaultPartition, pid)
			index[pid] = idefaultPartition
		}
	}
	if len(defaultPartition) > 0 {
		partitions = append(partitions, defaultPartition)
	}

	// Inform servers of the partitions they belong to
	for pid, srv := range mc.Servers {
		srv.Partition(partitions[index[pid]])
	}

	return nil
}

func (mc *MockCluster) Heal() {
	mc.Lock()
	for _, srv := range mc.Servers {
		srv.Heal()
	}
	mc.Unlock()
}

func (ms *MockServer) Heal() {
	ms.Lock()
	ms.peers = make(map[int]*MockServer, len(ms.allpeers))
	for pid, peer := range ms.allpeers {
		ms.peers[pid] = peer
	}
	ms.Unlock()
}

func (ms *MockServer) Pid() int { return ms.pid }

func (ms *MockServer) Peers() []int {
	pids := make([]int, len(ms.allpeers))
	i := 0
	for _, p := range ms.allpeers {
		pids[i] = p.pid
		i++
	}
	return pids
}

func (ms *MockServer) Outbox() chan *Envelope {
	return ms.outbox
}

func (ms *MockServer) Inbox() chan *Envelope {
	return ms.inbox
}

func (ms *MockServer) Cluster() *MockCluster {
	return ms.mc
}


func (ms *MockServer) AddPeer(peer *MockServer) {
	ms.Lock()
	ms.peers[peer.Pid()] = peer
	ms.allpeers[peer.Pid()] = peer
	ms.Unlock()
}

func (ms *MockServer) SetDropProbability(prob float32) {
	ms.Lock()
	ms.dropProbability = prob
	ms.Unlock()
}

func (ms *MockServer) SetMaxMessageDelay(delay int) {
	ms.Lock()
	ms.maxMessageDelayMs = delay
	ms.Unlock()
}

func (ms *MockServer) Partition(peerPids []int) {
	ms.Lock()
	peers := make(map[int]*MockServer)
	for _, pid := range peerPids {
		if peer, ok := ms.allpeers[pid]; ok {
			peers[pid] = peer
		}
	}
	ms.peers = peers
	ms.Unlock()
}

func (ms *MockServer) Close() {
	ms.Lock()
	ms.doClose(/*single server close */ false)
	ms.Unlock()
}

func (ms *MockServer) doClose(clusterClose bool) {
	if ms.closed {
		return
	}
	ms.closed = true
	ms.outbox <- QUIT
	ms.clusterbox <- QUIT
	if (! clusterClose) {
		ms.mc.removeServer(ms.pid)
	}
}

func (ms *MockServer) IsClosed() bool {
	ms.Lock()
	defer ms.Unlock()
	return ms.closed
}

func (ms *MockServer) sendForever() {
	for {
		e := <-ms.outbox
		if e == QUIT {
			return 
		}
		//fmt.Printf("SENDING @%d %+v\n", ms.pid, e.Msg)
		
		ms.Lock()
		peers := ms.peers
		ms.Unlock()

		for _, p := range peers {
			if e.Pid == BROADCAST {
				var env = *e // copy
				env.Pid = p.Pid()
				ms.send(&env)
			} else if e.Pid == p.Pid() {
				ms.send(e)
				break
			}
		}
	}
}

func (ms *MockServer) send(env *Envelope) {
	ms.Lock()
	peer, peerAvail := ms.peers[env.Pid]
	dontSendProb := ms.dropProbability
	delay := ms.maxMessageDelayMs
	ms.Unlock()
	drop := rand.Float32() <= dontSendProb

	if peerAvail && !drop {
		// Change 'to' address to 'from' address so that the recipient can reply using the same envelope
		env.Pid = ms.pid
		if delay > 0 {
			time.AfterFunc((time.Duration(rand.Intn(delay)) * time.Millisecond),
				func() { 
					peer.clusterbox <- env 
				})
		} else {
			peer.clusterbox <- env
		}

	}
}

// receive from cluster
func (ms *MockServer) receiveForever() {
	for {
		msg := <- ms.clusterbox
		//fmt.Printf("RECEIVED @%d: %+v\n",  ms.pid, msg.Msg)
		if msg == QUIT {
			return
		}
		ms.inbox <- msg
	}
}
