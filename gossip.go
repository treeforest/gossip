package gossip

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/treeforest/gossip/pb"
	log "github.com/treeforest/logger"
	"google.golang.org/grpc"
	"io"
	"math/rand"
	"net"
	"time"
)

type Gossip interface {
	Join(existing []string)
	Close()
	Go(id string, data []byte) error
	SetMeta(meta []byte)
	GetMeta() []byte
}

type gossipImpl struct {
	pb.UnimplementedGossipServer
	server            *grpc.Server
	config            *Config
	ms                *membership               // 成员管理
	userMsgChan       chan *pb.Envelope         // 用户消息通道
	meta              []byte                    // 元数据
	broadcastMsgChan  chan *pb.BroadcastMessage // 广播消息通道
	broadcastMidCache *Cache                    // 广播消息id缓存
	cancel            context.CancelFunc
}

func New(config *Config) Gossip {
	g := &gossipImpl{
		ms:                newMembership(),
		config:            config,
		userMsgChan:       make(chan *pb.Envelope, 128),
		broadcastMsgChan:  make(chan *pb.BroadcastMessage, 128),
		broadcastMidCache: NewCache(config.GossipMidCacheCap, config.GossipMidCacheTimeout),
	}

	log.Infof("node id: %s", config.Id)

	g.runGrpcServer()

	ctx, cancel := context.WithCancel(context.Background())
	g.cancel = cancel

	go g.dispatch(ctx)
	go g.schedule(ctx)

	return g
}

func (g *gossipImpl) runGrpcServer() {
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", g.config.BindAddress, g.config.BindPort))
	if err != nil {
		log.Fatalf("listen %s failed:%v", g.config.BindAddress, err)
	}

	g.server = grpc.NewServer(g.config.ServerOptions...)
	pb.RegisterGossipServer(g.server, g)
	log.Info("register gossip server...")

	go func() {
		log.Infof("serve at %s", lis.Addr().String())
		if err = g.server.Serve(lis); err != nil {
			log.Fatalf("serve failed: %v", err)
		}
	}()
	time.Sleep(time.Millisecond * 100)
}

func (g *gossipImpl) SetMeta(meta []byte) {
	g.meta = meta
}

func (g *gossipImpl) GetMeta() []byte {
	return g.meta
}

func (g *gossipImpl) Go(nodeId string, data []byte) error {
	m, ok := g.ms.Load(nodeId)
	if !ok {
		return errors.New("not found")
	}

	return m.sendUserMsg(data)
}

func (g *gossipImpl) Close() {
	g.cancel()
	g.ms.Leave(g.config.Id)
	g.server.Stop()
}

func (g *gossipImpl) Join(existing []string) {
	if len(existing) == 0 {
		return
	}

	for _, addr := range existing {
		if addr == "" {
			continue
		}

		m := newMember(nil, addr, g.config.DialTimeout, g.config.DialOptions)

		if err := m.connect(); err != nil {
			log.Warnf("connect failed: (addr=%s, err=%v)", addr, err)
			continue
		}

		stream, err := m.getPushPullStream()
		if err != nil {
			log.Warnf("get pushPull stream failed: (addr=%s, err=%v)", addr, err)
			continue
		}

		err = g.sendLocalState(stream, true)
		if err != nil {
			log.Warnf("dispatch local state failed: (addr=%s, err=%v)", addr, err)
			continue
		}

		if err = stream.CloseSend(); err != nil {
			log.Warnf("close send stream failed: (addr=%s, err=%v)", addr, err)
			continue
		}

		state, _, err := g.mergeRemoteState(stream)
		if err != nil {
			log.Warnf("merge remote state failed: (addr=%s, err=%v)", addr, err)
			continue
		}

		m.Update(state.Node)
		g.ms.Store(m.Id, m)
	}
}

func (g *gossipImpl) PushPull(srv pb.Gossip_PushPullServer) error {
	_, join, err := g.mergeRemoteState(srv)
	if err != nil {
		log.Errorf("merge remote state failed: %v", err)
		return err
	}

	err = g.sendLocalState(srv, join)
	if err != nil {
		log.Errorf("dispatch local state failed: %v", err)
		return err
	}

	return nil // 返回nil，标记服务端流结束
}

func (g *gossipImpl) Ping(ctx context.Context, req *pb.PingReq) (*pb.PingResp, error) {
	return &pb.PingResp{}, nil
}

func (g *gossipImpl) Send(ctx context.Context, req *pb.Envelope) (*pb.Empty, error) {
	g.userMsgChan <- req
	return &pb.Empty{}, nil
}

func (g *gossipImpl) Broadcast(ctx context.Context, msg *pb.BroadcastMessage) (*pb.Empty, error) {
	if !g.broadcastMidCache.Has(msg.Mid) { // 对缓存中已记录的消息id，不进行处理
		g.broadcastMidCache.Set(msg.Mid) // 标记将不再处理该消息
		g.userMsgChan <- &msg.Env        // 投递消息给用户
		g.broadcastMsgChan <- msg        // 继续转发
	}
	return &pb.Empty{}, nil
}

func (g *gossipImpl) Leave(ctx context.Context, req *pb.LeaveReq) (*pb.Empty, error) {
	m, ok := g.ms.Load(req.Id)
	if ok {
		g.ms.Delete(req.Id)
		if g.config.EventDelegate != nil {
			m.setState(pb.NodeStateType_Left)
			go g.config.EventDelegate.NotifyLeave(&m.Node)
		}
	}
	return &pb.Empty{}, nil
}

func (g *gossipImpl) MemberShip(ctx context.Context, req *pb.MembershipReq) (*pb.MembershipResp, error) {
	resp := &pb.MembershipResp{Nodes: []pb.Node{}}
	g.ms.Range(func(m *member) bool {
		if m.state == pb.NodeStateType_Alive {
			resp.Nodes = append(resp.Nodes, m.Node)
		}
		return true
	})
	return resp, nil
}

// dispatch 分发用户数据
func (g *gossipImpl) dispatch(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case env := <-g.userMsgChan:
			d := g.config.Delegate
			if d != nil {
				d.NotifyMsg(env.Payload)
			}
		}
	}
}

func (g *gossipImpl) schedule(ctx context.Context) {
	if g.config.ProbeInterval > 0 {
		go g.triggerFunc(ctx, g.config.ProbeInterval, g.probe)
	}

	if g.config.GossipInterval > 0 && g.config.Delegate != nil {
		go g.triggerFunc(ctx, g.config.GossipInterval, g.gossip)
	}

	if g.config.PushPullInterval > 0 {
		go g.triggerFunc(ctx, g.config.PushPullInterval, g.pushPull)
	}

	g.pullMembership()
	go g.triggerFunc(ctx, g.config.PullMembershipInterval, g.pullMembership)
}

// probe 检测节点状态
func (g *gossipImpl) probe() {
	g.ms.Range(func(m *member) bool {
		go func(m *member) {
			switch m.State() {
			case pb.NodeStateType_Suspect:
				if err := m.connect(); err != nil {
					m.setState(pb.NodeStateType_Dead)
					break
				}
				if m.setState(pb.NodeStateType_Alive) {
					m.UpdateStateChange()
					if g.config.EventDelegate != nil {
						go g.config.EventDelegate.NotifyJoin(m.GetNode())
					}
				}

			case pb.NodeStateType_Alive:
				if time.Now().Sub(m.StateChange()) > g.config.ProbeTimeout {
					m.setState(pb.NodeStateType_Dead)
					break
				}
				// send ping message
				if err := m.ping(); err != nil {
					// 可能网络丢包导致收包失败

					// 尝试重连
					if err = m.reconnect(); err != nil {
						break
					}
					m.UpdateStateChange()
				}
				m.setState(pb.NodeStateType_Alive)

			case pb.NodeStateType_Dead:
				// 尝试重连
				if err := m.reconnect(); err == nil {
					if m.setState(pb.NodeStateType_Alive) {
						m.UpdateStateChange()
						if g.config.EventDelegate != nil {
							go g.config.EventDelegate.NotifyJoin(m.GetNode())
						}
					}
					break
				}

				if time.Now().Sub(m.StateChange()) > g.config.ProbeTimeout*3 {
					if m.setState(pb.NodeStateType_Left) {
						if g.config.EventDelegate != nil {
							go g.config.EventDelegate.NotifyLeave(m.GetNode())
						}
					}
				}
			}
		}(m)
		return true
	})
}

// gossip 消息分发
func (g *gossipImpl) gossip() {
	mbs := g.ms.Alive(g.config.GossipNodes)

	if len(mbs) == 0 {
		return
	}

	msgs := make([]*pb.BroadcastMessage, 0)

	broadcasts := g.config.Delegate.GetBroadcasts()
	for _, payload := range broadcasts {
		mid := uuid.New().String()
		msgs = append(msgs, &pb.BroadcastMessage{
			SrcId: "", // 初始消息，没有来源或者说来源是自身
			Mid:   mid,
			Env:   pb.Envelope{Payload: payload},
		})
		g.broadcastMidCache.Set(mid) // 将不会转发给自己
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*20)
	defer cancel()
	done := make(chan struct{}, 1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				done <- struct{}{}
				return
			case msg := <-g.broadcastMsgChan:
				msgs = append(msgs, msg)
			}
		}
	}()
	<-done

	if len(msgs) == 0 {
		return
	}

	for _, m := range mbs {
		for _, msg := range msgs {
			if msg.SrcId == m.Id {
				// 不向源节点转发消息
				continue
			}
			msg.SrcId = g.config.Id // 设置源节点ID

			m.sendBroadcastMessage(msg)
		}
	}
}

// pushPull 推拉的方式同步节点状态
func (g *gossipImpl) pushPull() {
	m := g.ms.Rand()
	if m == nil {
		return
	}

	log.Debugf("pushPull from:%s", m.FullAddress())

	stream, err := m.getPushPullStream()
	if err != nil {
		log.Debugf("get pushPull stream failed: %v", err)
		return
	}

	err = g.sendLocalState(stream, false)
	if err != nil {
		log.Errorf("send local state failed: %v", err)
		return
	}

	if err = stream.CloseSend(); err != nil {
		log.Errorf("close send stream failed: %v", err)
		return
	}

	_, _, err = g.mergeRemoteState(stream)
	if err != nil {
		log.Errorf("merge remote state failed: %v", err)
	}
}

// pullMembership 拉取成员列表
func (g *gossipImpl) pullMembership() {
	m := g.ms.Rand()
	if m == nil {
		return
	}

	resp, err := m.sendMembershipRequest()
	if err != nil {
		log.Debugf("send membership request failed:%v", err)
		return
	}

	d := g.config.EventDelegate

	for _, node := range resp.Nodes {
		if node.Id == g.config.Id {
			continue
		}

		if mm, ok := g.ms.Load(node.Id); ok {
			if mm.Update(node) {
				if d != nil {
					d.NotifyUpdate(&node)
				}
			}
			continue
		}

		g.ms.Store(node.Id, newMember(&node, node.FullAddress(), g.config.DialTimeout, g.config.DialOptions))
	}
}

// triggerFunc 定时触发函数
func (g *gossipImpl) triggerFunc(ctx context.Context, stagger time.Duration, f func()) {
	randStagger := time.Duration(uint64(rand.Int63()) % uint64(stagger))
	select {
	case <-time.After(randStagger):
	case <-ctx.Done():
		return
	}

	t := time.NewTicker(stagger)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			f()
		}
	}
}

type Stream interface {
	Send(*pb.Envelope) error
	Recv() (*pb.Envelope, error)
}

// sendLocalState 发送本地状态
func (g *gossipImpl) sendLocalState(stream Stream, join bool) error {
	// 发送本地节点状态信息
	localState := pb.State{
		Node: pb.Node{
			Id:    g.config.Id,
			Name:  g.config.Name,
			Ip:    g.config.BindAddress,
			Port:  g.config.BindPort,
			Meta:  g.meta,
			State: pb.NodeStateType_Alive,
		},
		Join: join,
	}
	data, _ := localState.Marshal()
	err := stream.Send(&pb.Envelope{Payload: data})
	if err != nil {
		return fmt.Errorf("dispatch local state failed: %v", err)
	}

	d := g.config.Delegate
	if d == nil {
		return nil
	}

	// 发送用户数据
	userData := d.LocalState(join)
	if len(userData) == 0 {
		return nil
	}

	reader := bytes.NewReader(userData)
	payload := make([]byte, 1024*64)
	for {
		var n int
		n, err = reader.Read(payload)
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("read local user data failed: %v", err)
		}
		err = stream.Send(&pb.Envelope{Payload: payload[:n]})
		if err != nil {
			return err
		}
	}

	return nil
}

// mergeRemoteState 合并远端状态
func (g *gossipImpl) mergeRemoteState(stream Stream) (state *pb.State, join bool, err error) {
	env, err := stream.Recv()
	if err != nil {
		if err != io.EOF {
			return nil, false, fmt.Errorf("recv remote state failed:%v", err)
		}
	}

	var remoteState pb.State
	if err = remoteState.Unmarshal(env.Payload); err != nil {
		return nil, false, fmt.Errorf("unmarshal state failed: %v", err)
	}

	// 更新远端状态
	id := remoteState.Node.Id
	m, ok := g.ms.Load(id)
	if !ok {
		m = newMember(&remoteState.Node, remoteState.Node.FullAddress(),
			g.config.DialTimeout, g.config.DialOptions)
		g.ms.Store(id, m)

	} else {
		if m.Id == g.config.Id {
			// id 冲突
			return nil, false, fmt.Errorf("conflict with same node id %s", m.Id)
		}

		if m.Update(remoteState.Node) {
			if g.config.EventDelegate != nil {
				// 节点更新
				g.config.EventDelegate.NotifyUpdate(m.GetNode())
			}
		}
	}

	// 接收与合并状态信息
	buf := make([]byte, 0)
	for {
		env, err = stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, false, fmt.Errorf("recv user data failed:%v", err)
		}
		buf = append(buf, env.Payload...)
	}

	d := g.config.Delegate
	if d != nil {
		d.MergeRemoteState(buf, remoteState.Join)
	}

	return &remoteState, remoteState.Join, nil
}

func timeoutFunc(timeout time.Duration, fn func() error) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- fn()
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return errors.New("timeout")
	}
}
