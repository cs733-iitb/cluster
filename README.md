#cluster
cluster is a simple peer-to-peer messaging library for servers. The messaging abstraction resembles an inter-office system. To send a message, a server puts it in an `Envelope`, addresses it to the id of the destination server, and puts the envelope in its outbox.   It shows up in the destination's inbox. 

# Usage

```go
     import "github.com/cs733-iitb/cluster"

    config := cluster.Config{
        Peers: []cluster.PeerConfig{
            {Id: 100, Address: "localhost:7070"},
            {Id: 200, Address: "localhost:8080"},
            {Id: 300, Address: "localhost:9090"},
        }}

    // Or create it with a config object
    server1, _ := cluster.New(100, config)
    server2, _ := cluster.New(200, config) 

    // To send a message to server 200,
    server1.Outbox() <- &cluster.Envelope{Pid: 200, Msg: "Unicast to 200"}

    // To broadcast a message to all members of the cluster
    server1.Outbox() <- &cluster.Envelope{Pid: cluster.BROADCAST, Msg: "Broadcast"}

    // on the receiving end,
    env := <-server2.Inbox()

```

# Cluster configuration

Configuration can be supplied to cluster.New() either as a json file or as a cluster.Config structure. cluster.New takes the current node id to identify the port on which it listens.

     {"Servers":[
              {"Id":100,  "Address":"localhost:8001"},
              {"Id":200,  "Address":"localhost:8002"}
              {"Id":300,  "Address":"localhost:8003"}
           ],
       InboxSize: 1000,
       OutboxSize: 1000
     }

# Mock cluster testing

Separately, there is support for mock network testing that simulates network partitions, reordered messages etc. The real network is not used; this package is only meant to simulate a cluster in a single process. Each "server" runs in its own process.

```go
    import "github.com/cs733-iitb/cluster/mock"
    cl := mock.NewCluster()
    s100 := cl.AddServer(100)
    s200 := cl.AddServer(200)
    s100.Outbox() <- &Envelope{Pid: 200, Msg: "my first message"}
    env := <- s200.Inbox()

    // Inducing partitions.
    cl.Partition([]int{100, 200, 300}, []int{400, 500}) // Cluster partitions into two.

    // Heal partition
    cl.Heal()
```

# Installation and Dependencies.

    go get cs733-iit/cs733/cluster
    go test -race cs733-iit/cs733/cluster 

This library depends on the excellent zeromq package and the lovely zmq4 golang bindings. Thank you, Martin Sustrik,  Pieter Hintjens and Peter Kleiweg.

##Author: Sriram Srinivasan. sriram _at_ malhar.net
