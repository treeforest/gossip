package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/treeforest/gossip"
	"io"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"
)

type Message struct {
	Mid  int64
	From string
	Time time.Time
	Data string
}

func (m Message) Marshal() []byte {
	data, _ := json.Marshal(m)
	return data
}

func (m *Message) Unmarshal(data []byte) {
	_ = json.Unmarshal(data, m)
}

func (m *Message) Print() {
	fmt.Printf("%s\n\tfrom:%s\n\ttime:%s\n> ", m.Data, m.From, m.Time.Format("2006-01-02 15:04:05.99"))
}

type MessageCache struct {
	msgs   []*Message
	mp     map[int64]int
	locker sync.RWMutex
}

func (c *MessageCache) Data() []byte {
	c.locker.RLock()
	data, _ := json.Marshal(c.msgs)
	c.locker.RUnlock()
	return data
}

func (c *MessageCache) Exist(mid int64) bool {
	c.locker.RLock()
	_, ok := c.mp[mid]
	c.locker.RUnlock()
	return ok
}

func (c *MessageCache) Add(msg *Message) {
	if c.Exist(msg.Mid) {
		return
	}
	c.locker.Lock()
	c.mp[msg.Mid] = len(c.msgs)
	c.msgs = append(c.msgs, msg)
	c.locker.Unlock()
}

type Chat struct {
	cache *MessageCache
	queue [][]byte
	mu    sync.Mutex
}

func (c *Chat) NotifyMsg(data []byte) {
	if len(data) == 0 {
		return
	}
	var msg Message
	msg.Unmarshal(data)
	c.cache.Add(&msg)
	// msg.Print()
	fmt.Printf("%s\n> ", msg.Data)
}

func (c *Chat) GetBroadcasts() [][]byte {
	c.mu.Lock()
	broadcasts := c.queue
	c.queue = c.queue[:0]
	c.mu.Unlock()
	return broadcasts
}

func (c *Chat) LocalState(join bool) []byte {
	if !join {
		return []byte{}
	}

	return c.cache.Data()
}

func (c *Chat) MergeRemoteState(buf []byte, join bool) {
	if len(buf) == 0 || !join {
		return
	}

	//time.Sleep(time.Millisecond * 300)
	msgs := make([]*Message, 0)
	_ = json.Unmarshal(buf, &msgs)
	for _, msg := range msgs {
		if c.cache.Exist(msg.Mid) {
			continue
		}
		// msg.Print()
		fmt.Printf("%s\n> ", msg.Data)
		c.cache.Add(msg)
	}
}

func (c *Chat) Send(nick, msg string) {
	m := Message{
		Mid:  time.Now().UnixNano(),
		From: nick,
		Time: time.Now(),
		Data: msg,
	}
	// fmt.Printf("\tfrom:%s\n\ttime:%s\n> ", m.From, m.Time.Format("2006-01-02 15:04:05.99"))
	fmt.Printf("> ")
	c.mu.Lock()
	c.queue = append(c.queue, m.Marshal())
	c.mu.Unlock()
}

func main() {
	member := flag.String("member", "", "existing member")
	port := flag.Int("port", 0, "listen port")
	nick := flag.String("nick", "小李", "nick name")
	flag.Parse()

	chat := &Chat{
		cache: &MessageCache{
			msgs: make([]*Message, 0),
			mp:   map[int64]int{},
		},
		queue: make([][]byte, 0),
	}

	conf := gossip.DefaultConfig()
	conf.BindPort = int32(*port)
	conf.Delegate = chat
	g := gossip.New(conf)

	fmt.Printf("> ")
	go func() {
		r := bufio.NewReader(os.Stdin)
		for {
			msg, err := r.ReadBytes('\n')
			if err != nil {
				if err == io.EOF {
					break
				}
				panic(err)
			}
			if (len(msg) == 1 && (msg[0] == byte(13) || msg[0] == byte(10))) ||
				(len(msg) == 2 && msg[0] == byte(13) && msg[1] == byte(10)) {
				fmt.Print("> ")
				continue
			}
			chat.Send(*nick, string(msg[:len(msg)-2]))
		}
	}()

	if len(*member) != 0 {
		g.Join(strings.Split(*member, ","))
	}

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, os.Kill)
	<-done
	g.Close()
	time.Sleep(time.Second)
}
