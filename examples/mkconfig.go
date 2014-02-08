package main

import (
	"encoding/json"
	"github.com/sriram-srinivasan/cluster"
	"bytes"
)

func main() {
	config := &cluster.Config{
		InboxSize: 100,
		OutboxSize: 1000,
                Peers: []cluster.PeerConfig{
                        cluster.PeerConfig{Id: 1, Address: "localhost:38838"},
                        cluster.PeerConfig{Id: 2, Address: "google.com:80"},
                }}
	
	var b bytes.Buffer 
	enc := json.NewEncoder(&b)
	err := enc.Encode(config)
	if err != nil {panic(err)}
	println(b.String())

}
