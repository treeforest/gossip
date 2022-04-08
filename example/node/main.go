package main

import (
	"flag"
	"fmt"
	"github.com/treeforest/gossip"
	"github.com/treeforest/gossip/pb"
	log "github.com/treeforest/logger"
	"os"
	"os/signal"
	"strings"
	"time"
)

type eventDelegate struct{}

func (d *eventDelegate) NotifyJoin(n *pb.Node) {
	log.Infof("node join, addr[%s] id[%s]", n.FullAddress(), n.GetId())
}

func (d *eventDelegate) NotifyLeave(n *pb.Node) {
	log.Infof("node leave, addr[%s] id[%s]", n.FullAddress(), n.GetId())
}

func (d *eventDelegate) NotifyUpdate(n *pb.Node) {
	//log.Infof("node update, addr[%s] id[%s]", n.FullAddress(), n.GetId())
}

type delegate struct {
	queue [][]byte
}

func (d *delegate) NotifyMsg(data []byte) {
	log.Infof("notify msg: %s", string(data))
}

func (d *delegate) GetBroadcasts() [][]byte {
	//log.Debug("GetBroadcasts")
	msgs := d.queue
	d.queue = make([][]byte, 0)
	return msgs
}

func (d *delegate) LocalState(join bool) []byte {
	if join {
		return []byte("join == true")
	}
	return []byte("join == false")
}

func (d *delegate) MergeRemoteState(buf []byte, join bool) {
	// log.Infof("MergeRemoteState: %s", string(buf))
}

func (d *delegate) Broadcasts(data []byte) {
	if len(d.queue) > 32 {
		return
	}
	d.queue = append(d.queue, data)
}

func main() {
	member := flag.String("member", "", "existing member")
	port := flag.Int("port", 0, "listen port")
	flag.Parse()

	d := &delegate{queue: make([][]byte, 0)}
	conf := gossip.DefaultConfig()
	conf.BindAddress = "0.0.0.0"
	conf.BindPort = int32(*port)
	conf.Delegate = d
	conf.EventDelegate = &eventDelegate{}
	conf.GossipNodes = 2

	m := gossip.New(conf)
	if len(*member) != 0 {
		m.Join(strings.Split(*member, ","))
	}

	go func() {
		t := time.NewTicker(time.Second * 1)
		for {
			select {
			case <-t.C:
				d.Broadcasts([]byte(fmt.Sprintf("%s:%d -> %s", conf.BindAddress, conf.BindPort, time.Now().String())))
			}
		}
	}()

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, os.Kill)
	<-done
	m.Close()
	time.Sleep(time.Second)
}
