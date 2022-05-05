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
	"google.golang.org/grpc/connectivity"
	"io"
	"math/rand"
	"net"
	"sync"
	"time"
)

type Gossip interface {
	Join(existing []string)
	Close()
	SendTo(nodeId string, data []byte) error
	SetMeta(meta []byte)
	GetMeta() []byte
}

type nodeState struct {
	pb.Node
	State       pb.NodeStateType // 当前的状态
	StateChange time.Time        // 最新状态改变的时间
	locker      sync.RWMutex
}

type gossipImpl struct {
	pb.UnimplementedGossipServer
	config            *Config
	nodeStore         *sync.Map                 // 节点管理 id -> *nodeState
	connStore         *sync.Map                 // 连接管理 id -> *grpc.ClientConn
	userMsgChan       chan *pb.Envelope         // 用户消息通道
	meta              []byte                    // 元数据
	broadcastMsgChan  chan *pb.BroadcastMessage // 广播消息通道
	broadcastMidCache *Cache                    // 广播消息id缓存
	cancel            context.CancelFunc
}

func New(config *Config) Gossip {
	m := &gossipImpl{
		config:            config,
		nodeStore:         &sync.Map{},
		connStore:         &sync.Map{},
		userMsgChan:       make(chan *pb.Envelope, 128),
		broadcastMsgChan:  make(chan *pb.BroadcastMessage, 128),
		broadcastMidCache: NewCache(config.GossipMidCacheCap, config.GossipMidCacheTimeout),
	}

	log.Infof("node id: %s", config.Id)

	m.runGrpcServer()

	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel

	go m.dispatch(ctx)
	go m.schedule(ctx)

	return m
}

func (g *gossipImpl) runGrpcServer() {
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", g.config.BindAddress, g.config.BindPort))
	if err != nil {
		log.Fatalf("listen %s failed:%v", g.config.BindAddress, err)
	}

	server := grpc.NewServer(g.config.ServerOptions...)
	pb.RegisterGossipServer(server, g)
	log.Info("register gossip server...")

	go func() {
		log.Infof("serve at %s", lis.Addr().String())
		if err = server.Serve(lis); err != nil {
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

func (g *gossipImpl) SendTo(nodeId string, data []byte) error {
	val, ok := g.connStore.Load(nodeId)
	if !ok {
		return errors.New("not found")
	}
	cc := val.(*grpc.ClientConn)
	_, err := pb.NewGossipClient(cc).Send(context.Background(), &pb.Envelope{Payload: data})
	return err
}

func (g *gossipImpl) Close() {
	g.cancel()
	g.connStore.Range(func(key, value any) bool {
		cc := value.(*grpc.ClientConn)
		_, _ = pb.NewGossipClient(cc).Leave(context.Background(), &pb.LeaveReq{Id: g.config.Id})
		_ = cc.Close()
		return true
	})
}

func (g *gossipImpl) Join(existing []string) {
	if len(existing) == 0 {
		return
	}

	for _, addr := range existing {
		cc, err := g.dial(addr)
		if err != nil {
			log.Errorf("connect %s failed: %v", addr, err)
			continue
		}

		stream, err := pb.NewGossipClient(cc).PushPull(context.Background())
		if err != nil {
			log.Errorf("get pushPull stream failed: %v, state:%d", err, cc.GetState())
			continue
		}

		err = g.sendLocalState(stream, true)
		if err != nil {
			log.Errorf("send local state failed: %v", err)
			continue
		}

		if err = stream.CloseSend(); err != nil {
			log.Errorf("close send failed: %v", err)
			continue
		}

		id, _, err := g.mergeRemoteState(stream)
		if err != nil {
			log.Errorf("merge remote state failed: %v", err)
			continue
		}

		g.connStore.Store(id, cc)
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
		log.Errorf("send local state failed: %v", err)
		return err
	}

	return nil // 返回nil，标记服务端流结束
}

func (g *gossipImpl) Ping(ctx context.Context, req *pb.Empty) (*pb.Empty, error) {
	return &pb.Empty{}, nil
}

func (g *gossipImpl) Send(ctx context.Context, req *pb.Envelope) (*pb.Empty, error) {
	g.userMsgChan <- req
	return &pb.Empty{}, nil
}

func (g *gossipImpl) Broadcast(ctx context.Context, msg *pb.BroadcastMessage) (*pb.Empty, error) {
	if !g.broadcastMidCache.Has(msg.GetMid()) { // 对缓存中已记录的消息id，不进行处理
		g.broadcastMidCache.Set(msg.GetMid()) // 标记将不再处理该消息
		g.userMsgChan <- msg.GetEnv()         // 投递消息给用户
		g.broadcastMsgChan <- msg             // 继续转发
	}
	return &pb.Empty{}, nil
}

func (g *gossipImpl) Leave(ctx context.Context, req *pb.LeaveReq) (*pb.Empty, error) {
	val, ok := g.nodeStore.Load(req.GetId())
	if ok {
		g.connStore.Delete(req.GetId())
		g.nodeStore.Delete(req.GetId())
		if g.config.EventDelegate != nil {
			state := val.(*nodeState)
			state.Node.State = pb.NodeStateType_Left
			go g.config.EventDelegate.NotifyLeave(&state.Node)
		}
	}
	return &pb.Empty{}, nil
}

func (g *gossipImpl) MemberShip(req *pb.Empty, srv pb.Gossip_MemberShipServer) error {
	g.nodeStore.Range(func(key, value any) bool {
		state := value.(*nodeState)
		err := srv.Send(&pb.State{Node: &state.Node, Join: false})
		if err != nil {
			log.Errorf("send membership failed: %v", err)
			return false
		}
		return true
	})
	return nil
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
				d.NotifyMsg(env.GetPayload())
			}
		}
	}
}

// dial 拨号
func (g *gossipImpl) dial(addr string) (*grpc.ClientConn, error) {
	var cc *grpc.ClientConn = nil

	err := g.timeoutFunc(g.config.DialTimeout, func() error {
		var err error
		cc, err = grpc.DialContext(context.Background(), addr, g.config.DialOptions...)
		return err
	})

	if err != nil {
		return nil, fmt.Errorf("dial %s failed: %v", addr, err)
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

	err = g.timeoutFunc(g.config.DialTimeout, func() error {
		_, err = pb.NewGossipClient(cc).Ping(context.Background(), &pb.Empty{})
		return err
	})

	if err != nil {
		return nil, fmt.Errorf("ping failed: %v", err)
	}

	return cc, nil
}

func (g *gossipImpl) schedule(ctx context.Context) {
	if g.config.ProbeInterval > 0 {
		go g.triggerFunc(ctx, g.config.ProbeInterval, g.probe)
	}

	if g.config.GossipInterval > 0 && g.config.GossipNodes > 0 && g.config.Delegate != nil {
		go g.triggerFunc(ctx, g.config.GossipInterval, g.gossip)
	}

	if g.config.PushPullInterval > 0 {
		go g.triggerFunc(ctx, g.config.PushPullInterval, g.pushPull)
	}

	go g.triggerFunc(ctx, time.Second, g.pullMembership)
}

// probe 检测节点状态
func (g *gossipImpl) probe() {
	ping := func(id string, state *nodeState) {
		val, ok := g.connStore.Load(id)
		if !ok {
			state.State = pb.NodeStateType_Suspect
			return
		}
		cc := val.(*grpc.ClientConn)
		err := g.timeoutFunc(g.config.ProbeTimeout, func() error {
			_, err := pb.NewGossipClient(cc).Ping(context.Background(), &pb.Empty{})
			return err
		})
		if err != nil {
			state.State = pb.NodeStateType_Dead
			g.connStore.Delete(state.GetId())
			return
		}
		state.State = pb.NodeStateType_Alive
	}

	g.nodeStore.Range(func(key, value any) bool {
		state := value.(*nodeState)
		switch state.State {
		case pb.NodeStateType_Alive:
			ping(state.GetId(), state) // 发送ping消息
		case pb.NodeStateType_Suspect:
			cc, err := g.dial(state.FullAddress())
			if err != nil {
				// 连接失败
				if time.Now().Sub(state.StateChange) < time.Second*30 {
					// 状态改变时间是30s内，可能是不同分区的节点，无法连接。
					// 当超时30s状态未更新再改变状态为Dead
					break
				}
				state.State = pb.NodeStateType_Dead
			} else {
				state.State = pb.NodeStateType_Alive
				g.connStore.Store(key, cc)
			}
		case pb.NodeStateType_Dead:
			if time.Now().Sub(state.StateChange) < time.Second*30 {
				state.State = pb.NodeStateType_Suspect
			}
		case pb.NodeStateType_Left:
			g.nodeStore.Delete(key)
			g.connStore.Delete(key)
			return true
		}

		g.nodeStore.Store(key, state)
		return true
	})
}

// gossip 消息分发
func (g *gossipImpl) gossip() {
	num := g.config.GossipNodes
	nodes := make([]pb.Node, 0)

	g.nodeStore.Range(func(key, value any) bool {
		state := value.(*nodeState)
		if state.State == pb.NodeStateType_Alive {
			nodes = append(nodes, state.Node)
			num--
			if num == 0 {
				return false
			}
		}
		return true
	})

	if len(nodes) == 0 {
		return
	}

	msgs := make([]*pb.BroadcastMessage, 0)

	broadcasts := g.config.Delegate.GetBroadcasts()
	for _, payload := range broadcasts {
		mid := uuid.New().String()
		msgs = append(msgs, &pb.BroadcastMessage{
			SrcId: "", // 初始消息，没有来源或者说来源是自身
			Mid:   mid,
			Env:   &pb.Envelope{Payload: payload},
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

	for _, node := range nodes {
		val, ok := g.connStore.Load(node.GetId())
		if !ok {
			continue
		}
		cc := val.(*grpc.ClientConn)
		for _, msg := range msgs {
			if msg.SrcId == node.GetId() {
				// 不向源节点转发消息
				continue
			}
			msg.SrcId = g.config.Id // 设置源节点ID
			_, err := pb.NewGossipClient(cc).Broadcast(context.Background(), msg)
			if err != nil {
				log.Warnf("gossip failed: %v", err)
				break
			}
		}
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
		case <-t.C:
			f()
		case <-ctx.Done():
			return
		}
	}
}

// pushPull 推拉的方式同步节点状态
func (g *gossipImpl) pushPull() {
	_, cc := g.getRandConnectedNode()
	if cc == nil {
		return
	}

	stream, err := pb.NewGossipClient(cc).PushPull(context.Background())
	if err != nil {
		log.Warnf("pushPull failed: %v", err)
		return
	}

	err = g.sendLocalState(stream, false)
	if err != nil {
		log.Errorf("send local state failed: %v", err)
		return
	}

	_ = stream.CloseSend()

	_, _, err = g.mergeRemoteState(stream)
	if err != nil {
		log.Errorf("merge remote state failed: %v", err)
	}
}

// pullMembership 拉取成员列表
func (g *gossipImpl) pullMembership() {
	_, cc := g.getRandConnectedNode()
	if cc == nil {
		return
	}

	stream, err := pb.NewGossipClient(cc).MemberShip(context.Background(), &pb.Empty{})
	if err != nil {
		log.Warnf("get membership stream failed:%v", err)
		return
	}

	for {
		state, err := stream.Recv()
		if err != nil {
			if err != io.EOF {
				log.Errorf("stream recv failed: %v", err)
			}
			break
		}
		if state.GetNode().GetId() == g.config.Id {
			continue
		}
		_, ok := g.nodeStore.Load(state.GetNode().GetId())
		if ok {
			continue
		}
		g.Join([]string{state.GetNode().FullAddress()})
	}
}

// getRandConnectedNode 随机获取一个已连接的节点
func (g *gossipImpl) getRandConnectedNode() (*nodeState, *grpc.ClientConn) {
	var (
		node *nodeState       = nil
		cc   *grpc.ClientConn = nil
	)
	g.nodeStore.Range(func(key, value any) bool {
		node = value.(*nodeState)
		if node.State == pb.NodeStateType_Alive {
			val, ok := g.connStore.Load(node.GetId())
			if !ok {
				return true
			}
			cc = val.(*grpc.ClientConn)
			return false
		}
		node = nil
		return true
	})
	return node, cc
}

type Stream interface {
	Send(*pb.Envelope) error
	Recv() (*pb.Envelope, error)
}

// sendLocalState 发送本地状态
func (g *gossipImpl) sendLocalState(stream Stream, join bool) error {
	// 发送本地节点状态信息
	localState := pb.State{
		Node: &pb.Node{
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
		return fmt.Errorf("send local state failed: %v", err)
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
		n, err := reader.Read(payload)
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
func (g *gossipImpl) mergeRemoteState(stream Stream) (id string, join bool, err error) {
	env, err := stream.Recv()
	if err != nil {
		if err != io.EOF {
			return "", false, fmt.Errorf("recv remote state failed:%v", err)
		}
	}

	var remoteState pb.State
	if err = remoteState.Unmarshal(env.GetPayload()); err != nil {
		return "", false, fmt.Errorf("unmarshal state failed: %v", err)
	}

	// 更新远端状态
	id = remoteState.GetNode().GetId()
	val, ok := g.nodeStore.Load(id)
	if !ok {
		g.nodeStore.Store(id, &nodeState{
			Node:        *remoteState.GetNode(),
			State:       pb.NodeStateType_Suspect, // 刚加入，还没进行连接，状态为suspect
			StateChange: time.Now(),
		})
	} else {
		state := val.(*nodeState)
		if state.GetId() == g.config.Id && state.State == pb.NodeStateType_Alive {
			// id 冲突
			return "", false, fmt.Errorf("conflict with same node id %s", state.GetId())
		}
		state.StateChange = time.Now()
		if !bytes.Equal(state.GetMeta(), remoteState.GetNode().GetMeta()) {
			state.Meta = remoteState.GetNode().GetMeta()
			if g.config.EventDelegate != nil {
				// 节点更新
				g.config.EventDelegate.NotifyUpdate(&state.Node)
			}
		}
		g.nodeStore.Store(id, state)
	}

	if g.config.EventDelegate != nil {
		node := *remoteState.GetNode()
		if remoteState.GetJoin() {
			// 节点加入
			g.config.EventDelegate.NotifyJoin(&node)
		}
	}

	// 接收与合并状态信息
	userData := make([]byte, 0)
	for {
		env, err = stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return "", false, fmt.Errorf("recv user data failed:%v", err)
		}
		userData = append(userData, env.GetPayload()...)
	}

	d := g.config.Delegate
	if d != nil {
		d.MergeRemoteState(userData, remoteState.GetJoin())
	}

	return id, remoteState.GetJoin(), nil
}

func (g *gossipImpl) timeoutFunc(timeout time.Duration, fn func() error) error {
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
