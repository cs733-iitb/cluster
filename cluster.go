
// package cluster provides facilities for unicast and broadcast messaging between members of a cluster.
// It uses zeromq (in particular, github.com/pebbe/zmq4) to deal with connection establishment and reestablishment
// Each server creates a zeromq PULL socket for its own address, and a PUSH socket for all.
// A cluster is defined by all the servers in a configuration file (in JSON format) as follows:
//
//     {"Servers":[
//              {"Id":100,  "Address":"localhost:8001"},
//              {"Id":200,  "Address":"localhost:8002"}
//              {"Id":300,  "Address":"localhost:8003"}
//         ],
//       InboxSize: 1000,
//       OutboxSize: 1000
//     }
// 
// Example:
//      srvr := cluster.New(/*myid*/ 100, "test_config.json")
//
//  To send a message to server "300"
//       srvr.Outbox() <- &Envelope(Pid: 300, MsgId: 1, Msg: "This is a unicast")
//
//  To broadcast:
//       srvr.Outbox() <- &Envelope(Pid: cluster.BROADCAST, MsgId: 1, Msg: "This is a unicast")
//
//  To receive:
//      envelope := <- srvr.Inbox()
//
//  Messages are dropped if there is no room on the pipe. No errors are returned
package cluster

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/golang/glog"
	zmq "github.com/pebbe/zmq4"
	"os"
	"strings"
	"sync"
)

const (
	BROADCAST = -1
)

type Peer struct {
	mutex   sync.Mutex
	Id      int
	Address string
	sock    *zmq.Socket
	connected bool
}

type serverImpl struct { // implements Server interface
	Peer
	peers  []*Peer
	inbox  chan *Envelope
	outbox chan *Envelope
}

type PeerConfig struct {
	Id      int
	Address string
}

type Config struct {
	Peers      []PeerConfig
	InboxSize  int
	OutboxSize int
}

var QUIT *Envelope

func init() {
	fmt.Print() // to avoid having to comment out "import fmt"
	QUIT = &Envelope{Pid: -1, Msg: "'MockCluster_QUIT'"}
}

func New(myid int, configFile string) (server Server, err error) {
	var f *os.File
	if f, err = os.Open(configFile); err != nil {
		return
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	var config Config
	if err = dec.Decode(&config); err != nil {
		return
	}
/*
	if config.InboxSize == 0 {
		config.InboxSize = 100
	}
	if config.OutboxSize == 0 {
		config.OutboxSize = 100
	}
*/
	srvr := new(serverImpl)
	server = srvr
	srvr.inbox = make(chan *Envelope, config.InboxSize)
	srvr.outbox = make(chan *Envelope, config.OutboxSize)
	srvr.peers = []*Peer{}
	err = srvr.initEndPoints(myid, &config)
	return srvr, err
}

func (server *serverImpl) Pid() int { return server.Id }
func (server *serverImpl) Peers() []int {
	pids := make([]int, len(server.peers))
	for i, peer := range server.peers {
		pids[i] = peer.Id
	}
	return pids
}

func (server *serverImpl) Inbox() chan *Envelope  { return server.inbox }
func (server *serverImpl) Outbox() chan *Envelope { return server.outbox }

func (server *serverImpl) initEndPoints(myid int, config *Config) (err error) {
	foundMyId := false
	for _, srv := range config.Peers {
		if srv.Id == myid {
			foundMyId = true
			server.Id = myid
			port := strings.SplitAfter(srv.Address, ":")[1]
			sock, err := zmq.NewSocket(zmq.PULL)
			server.sock = sock
			err = sock.Bind("tcp://*:" + port)
			if err != nil {
				glog.Fatalf("Unable to bind a zmq socket to %s. Error=%v\n", srv.Address, err)
			}
			glog.Info("Listening at port " + port)
		} else {
			var peer Peer
			peer.Id = srv.Id
			peer.Address = srv.Address
			sock, err := zmq.NewSocket(zmq.PUSH)
			if err != nil {
				glog.Fatalln("Unable to create zmq socket\n")
			}
			peer.sock = sock
			//err = sock.Connect("tcp://" + srv.Address)
			//if err != nil {
//				glog.Fatalf("Error connecting to %s. Error=%v\n", srv.Address, err)
//			}
			server.peers = append(server.peers, &peer)
		}
	}
	if !foundMyId {
		glog.Fatalf("Expected this server's id (\"%d\") to be present in the configuration", myid)
	}
	go server.recvForever()
	go server.sendForever()
	return
}

func (server *serverImpl) recvForever() {
	var recvSock *zmq.Socket
	recvSock = server.sock

	for {
		packet, err := recvSock.RecvBytes(0) // no need to thread-protect this socket. No one else is using it
		if err == nil {
			buf := bytes.NewBuffer(packet)
			dec := gob.NewDecoder(buf)
			envelope := new(Envelope)
			err := dec.Decode(envelope)
			if err != nil {
				glog.Errorf("Error in decoding message: %v\n", err)
			} else {
				server.Inbox() <- envelope
			}
		} else {
			//glog.V(2).Infof("Error receiving packet : %v\n", err)
			glog.Errorf("Error receiving packet : %v\n", err)
		}
	}
}

func (envelope *Envelope) toBytes() (pkt []byte) {
	// insert in an envelope and gob encode it
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(envelope)
	if err == nil {
		pkt = buffer.Bytes()
	} else {
		glog.Errorf("Error encoding envelope: %+v\n", envelope)
	}
	return pkt
}

func (server *serverImpl) sendForever() {
	for {
		envelope := <-server.outbox
		toPid := envelope.Pid
		envelope.Pid = server.Id
		pkt := envelope.toBytes() // convert to bytes
		for _, peer := range server.peers {
			if toPid == BROADCAST {
				//fmt.Printf("SENDING to %d: %s\n", envelope.Pid, envelope.Msg)
				peer.sendPacket(pkt)
			} else if peer.Id == toPid {
				peer.sendPacket(pkt)
				break
			}
		}
	}
}

func (peer *Peer) sendPacket(pkt []byte) { // raw bytes interface, in
	if pkt == nil {
		return
	}

	peer.mutex.Lock()
	defer peer.mutex.Unlock()
	p := peer
	if (! p.connected) {
		p.sock.Connect("tcp://" + p.Address)
		p.connected = true

	}
	n, err := peer.sock.SendBytes(pkt, zmq.DONTWAIT)
	if err != nil || n == 0 {
		//glog.V(2).Infof("Error sending packet: %v", err)
		glog.Errorf("Error sending packet: %v", err)
	}
}
