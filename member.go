package gossip

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/treeforest/gossip/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"sync"
	"time"
)

type MemberError error

var (
	StopError   MemberError = errors.New("stopped")
	Unconnected MemberError = errors.New("unconnected")
	Suspect     MemberError = errors.New("suspect")
	Dead        MemberError = errors.New("dead")
	Left        MemberError = errors.New("leaved")
)

type member struct {
	pb.Node
	mutex       sync.RWMutex
	state       pb.NodeStateType // 当前的状态
	stateChange time.Time        // 最新状态改变的时间
	address     string
	dialOptions []grpc.DialOption
	dialTimeout time.Duration
	cc          *grpc.ClientConn
	initialize  bool
	stopped     chan struct{}
	ch          chan *event
}

type event struct {
	target   any
	retValue any
	c        chan error
}

func newMember(node *pb.Node, addr string, dialTimeout time.Duration,
	dialOptions []grpc.DialOption) *member {
	m := &member{
		state:       pb.NodeStateType_Suspect, // 刚加入，还没进行连接，状态为suspect
		stateChange: time.Now(),
		address:     addr,
		dialOptions: dialOptions,
		dialTimeout: dialTimeout,
		cc:          nil,
		stopped:     make(chan struct{}, 1),
		ch:          make(chan *event, 1024),
	}

	if node == nil {
		m.initialize = false
	} else {
		m.Node = *node
		m.initialize = true
	}

	go m.dispatch()

	return m
}

func (m *member) StateChange() time.Time {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.stateChange
}

func (m *member) UpdateStateChange() {
	m.mutex.Lock()
	m.stateChange = time.Now()
	m.mutex.Unlock()
}

func (m *member) GetNode() *pb.Node {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	node := m.Node
	return &node
}

func (m *member) Update(node pb.Node) bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.initialize == false {
		m.Node = node
		m.initialize = true
		return true
	}

	if m.Node.Equal(&node) {
		return false
	}

	if m.Node.State != node.State {
		m.Node.State = node.State
	}

	if m.Node.Name != node.Name {
		m.Node.Name = node.Name
	}

	if m.Node.Ip != node.Ip {
		m.Node.Ip = node.Ip
	}

	if m.Node.Port != node.Port {
		m.Node.Port = node.Port
	}

	m.Node.State = node.State

	if !bytes.Equal(m.Node.Meta, node.Meta) {
		m.Node.Meta = node.Meta
		return true
	}

	return false
}

func (m *member) State() pb.NodeStateType {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.state
}

func (m *member) setState(state pb.NodeStateType) bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.state == state {
		return false
	}
	m.state = state
	return true
}

func (m *member) close() {
	m.stopped <- struct{}{}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.cc != nil {
		_ = m.cc.Close()
		m.cc = nil
	}
}

func (m *member) sendBroadcastMessage(msg *pb.BroadcastMessage) {
	m.sendAsync(msg)
}

func (m *member) sendLeaveRequest(id string) error {
	_, err := m.send(&pb.LeaveReq{Id: id})
	return err
}

func (m *member) sendUserMsg(payload []byte) error {
	_, err := m.send(&pb.Envelope{Payload: payload})
	return err
}

func (m *member) getPushPullStream() (pb.Gossip_PushPullClient, error) {
	cli := m.client()
	if cli == nil {
		return nil, Unconnected
	}
	return cli.PushPull(context.Background())
}

func (m *member) sendMembershipRequest() (*pb.MembershipResp, error) {
	resp, err := m.send(&pb.MembershipReq{})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.MembershipResp), nil
}

func (m *member) ping() error {
	_, err := m.send(&pb.PingReq{})
	return err
}

func (m *member) send(target any) (any, error) {
	e := &event{target: target, c: make(chan error, 1)}

	select {
	case <-m.stopped:
	case m.ch <- e:
	}

	select {
	case <-m.stopped:
		return nil, StopError
	case err := <-e.c:
		return e.retValue, err
	}
}

func (m *member) sendAsync(v any) {
	e := &event{target: v, c: make(chan error, 1)}

	select {
	case m.ch <- e:
		return
	default:
	}

	go func() {
		select {
		case m.ch <- e:
		case <-m.stopped:
		}
	}()
}

func (m *member) dispatch() {
LOOP:
	for {
		select {
		case <-m.stopped:
			return
		case e := <-m.ch:
			cli := m.client()
			if cli == nil {
				e.c <- Unconnected
				break
			}

			switch m.State() {
			case pb.NodeStateType_Dead:
				e.c <- Dead
				goto LOOP
			case pb.NodeStateType_Left:
				e.c <- Left
				goto LOOP
			case pb.NodeStateType_Suspect:
				e.c <- Suspect
				goto LOOP
			}

			ctx := context.Background()
			var err error

			switch msg := e.target.(type) {
			case *pb.BroadcastMessage:
				e.retValue, err = cli.Broadcast(ctx, msg)
			case *pb.LeaveReq:
				e.retValue, err = cli.Leave(ctx, msg)
			case *pb.Envelope:
				e.retValue, err = cli.Send(ctx, msg)
			case *pb.PingReq:
				e.retValue, err = cli.Ping(ctx, msg)
			case *pb.MembershipReq:
				e.retValue, err = cli.MemberShip(ctx, msg)
			}

			if err == nil {
				m.UpdateStateChange()
			}

			e.c <- err
		}
	}
}

func (m *member) client() pb.GossipClient {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	if m.cc == nil {
		return nil
	}
	return pb.NewGossipClient(m.cc)
}

func (m *member) connect() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.cc != nil {
		return nil
	}

	var err error
	m.cc, err = m.dial()

	return err
}

func (m *member) reconnect() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	var err error
	for i := 0; i < 3; i++ {
		m.cc, err = m.dial()
		if err == nil {
			break
		}
	}
	return err
}

// dial 拨号
func (m *member) dial() (*grpc.ClientConn, error) {
	var cc *grpc.ClientConn = nil

	err := timeoutFunc(m.dialTimeout, func() error {
		var err error
		cc, err = grpc.DialContext(context.Background(), m.address, m.dialOptions...)
		return err
	})

	if err != nil {
		return nil, fmt.Errorf("dial %s failed: %v", m.address, err)
	}

	s := cc.GetState()
	switch s {
	case connectivity.Idle:
		break
	case connectivity.Connecting:
		break
	case connectivity.Ready:
		break
	case connectivity.TransientFailure:
		return nil, fmt.Errorf("transient failure")
	case connectivity.Shutdown:
		return nil, fmt.Errorf("connect shutdown")
	default:
		return nil, fmt.Errorf("unknown connectivity state: %d", s)
	}

	err = timeoutFunc(m.dialTimeout, func() error {
		_, err = pb.NewGossipClient(cc).Ping(context.Background(), &pb.PingReq{})
		return err
	})

	if err != nil {
		return nil, fmt.Errorf("connect failed: %v", err)
	}

	return cc, nil
}
