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
//       "InboxSize": 1000,
//       "OutboxSize": 1000
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
	"syscall"
	"time"
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"log"
	"os"
	"strings"
	"sync"
	"errors"
)

const (
	BROADCAST = -1
)

type Peer struct {
	sync.Mutex
	Id      int
	Address string
	sock    *zmq.Socket
	connected bool
	closed bool
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

func init() {
	fmt.Print() // to avoid having to comment out "import fmt"
}

// config can be the name of a json formatted file or a Config structure.
// A sample configuration looks like this:
// { "InboxSize": 100,
//   "Peers":[
//     {"Id":1,"Address":"localhost:8001"},
//     {"Id":2,"Address":"localhost:8002"},
//     {"Id":3,"Address":"localhost:8003"},
//     {"Id":4,"Address":"localhost:8004"},
//     {"Id":5,"Address":"localhost:8005"}
//   ]
// }

func New(myid int, configuration interface{}) (server Server, err error) {
	var config *Config
	if config, err = ToConfig(configuration); err != nil {
		return nil, err
	}
	if config.InboxSize == 0 {
		config.InboxSize = 10
	}
	if config.OutboxSize == 0 {
		config.OutboxSize = 10
	}
	srvr := new(serverImpl)
	server = srvr
	srvr.inbox = make(chan *Envelope, config.InboxSize)
	srvr.outbox = make(chan *Envelope, config.OutboxSize)
	srvr.peers = []*Peer{}
	err = srvr.initEndPoints(myid, config)
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

func (server *serverImpl) IsClosed() bool {
	server.Lock()
	defer server.Unlock()
	return server.closed
}

func (server *serverImpl) Close() {
	server.Lock()
	server.closed = true // server.recvForever is polling this flag
	close(server.outbox) // to make server.sendForever quit
	server.Unlock()
	time.Sleep(600 * time.Millisecond) // so that send/rcv timeouts kick in
}

func (server *serverImpl) initEndPoints(myid int, config *Config) (err error) {
	foundMyId := false
	for _, srv := range config.Peers {
		if srv.Id == myid {
			foundMyId = true
			server.Id = myid
			port := strings.SplitAfter(srv.Address, ":")[1]
			go server.recvForever(port)
		} else {
			var peer Peer
			peer.Id = srv.Id
			peer.Address = srv.Address
			server.peers = append(server.peers, &peer)
		}
	}
	if !foundMyId {
		log.Fatalf("Expected this server's id (\"%d\") to be present in the configuration", myid)
	}
	go server.sendForever()
	return
}

var EAGAIN = zmq.AsErrno(syscall.EAGAIN)
func (server *serverImpl) recvForever(port string) {
	var recvSock *zmq.Socket
	recvSock, err := zmq.NewSocket(zmq.PULL)
	if err == nil {
		err = recvSock.Bind("tcp://*:" + port)
		if err != nil {
			log.Fatalf("Unable to bind a zmq socket to %s. Error=%v\n", port, err)
		}
	}
	recvSock.SetLinger(0)
	defer func() {
		recvSock.Close()
	}()
	
	// Timeout every so often to check server.isClosed()
 	recvSock.SetRcvtimeo(500 * time.Millisecond)
	for {
		if server.IsClosed() {
			break
		}
		
		packet, err := recvSock.RecvBytes(0) // no need to thread-protect this socket. No one else is using it
		if err == nil {
			buf := bytes.NewBuffer(packet)
			dec := gob.NewDecoder(buf)
			envelope := new(Envelope)
			err := dec.Decode(envelope)
			if err != nil {
				log.Fatalf("Error in decoding message: %v\n", err)
			} else {
				server.inbox <- envelope
			}
		} else {
			if err != EAGAIN { // something other than a rcv timeout
				log.Printf("Error receiving packet : %v\n", err)
				break
			}
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
		log.Fatalf("Error encoding envelope: %+v\n%+v", err, envelope)
	}
	return pkt
}

func (server *serverImpl) sendForever() {
	// No mutexes used for peers or sockets because this is the only
	// routine that accesses them.
	for _, peer := range server.peers {
		sock, err := zmq.NewSocket(zmq.PUSH)
		if err != nil {
			log.Fatalln("Unable to create zmq socket\n")
		}
		peer.sock = sock
		err = sock.Connect("tcp://" + peer.Address)
		//sock.SetSndtimeo(500 * time.Millisecond)
		if err != nil {
			log.Fatalf("Error connecting to %s. Error=%v\n", peer.Address, err)
		}
		sock.SetLinger(0)
	}

LOOP:
	for {
		var envelope *Envelope
		var ok bool
		select {
		case envelope, ok = <-server.outbox:
			if !ok {
				break LOOP
			}
		case <- time.After(500 * time.Millisecond):
			if server.IsClosed() {
				break LOOP
			} else {
				continue LOOP
			}
		}

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
	for _, peer := range server.peers {
		peer.sock.Close()
		peer.connected = false
	}
}

func (peer *Peer) sendPacket(pkt []byte) { // raw bytes interface, in
	if pkt == nil {
		return
	}

	peer.Lock()
	defer peer.Unlock()
	p := peer
	if (! p.connected) {
		p.sock.Connect("tcp://" + p.Address)
		p.connected = true

	}
	n, err := peer.sock.SendBytes(pkt, zmq.DONTWAIT)
	if err != nil || n == 0 {
		log.Printf("Error sending packet: %v", err)
	}
}

func ToConfig(configuration interface{}) (config *Config, err error){
	var cfg Config
	var ok bool
	var configFile string
	if configFile, ok = configuration.(string); ok {
		var f *os.File
		if f, err = os.Open(configFile); err != nil {
			return nil, err
		}
		defer f.Close()
		dec := json.NewDecoder(f)
		if err = dec.Decode(&cfg); err != nil {
			return nil, err
		}
	} else if cfg, ok = configuration.(Config); !ok {
		return nil, errors.New("Expected a configuration.json file or a Config structure")
	}
	return &cfg, nil
}
