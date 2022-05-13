package gossip

import (
	"bytes"
	"context"
	"errors"
	"github.com/treeforest/gossip/pb"
	log "github.com/treeforest/logger"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type Gossip interface {
	// UpdateMetadata 更新元数据
	UpdateMetadata(metadata []byte)

	// GetMetadata 返回元数据
	GetMetadata() []byte

	// GetID 返回当前成员的id
	GetID() string

	// GetEndpoint 返回当前向外暴露的端点信息
	GetEndpoint() string

	// SendToPeer 向指定节点发送消息
	SendToPeer(peer *pb.RemotePeer, data []byte) ([]byte, error)

	// Probe 探测节点的存活状态
	Probe(peer *pb.RemotePeer) bool

	// Stop 停止gossip模块
	Stop()
}

var (
	ErrAlreadyReceived = errors.New("already received")
)

type gossipImpl struct {
	pb.UnimplementedGossipServer
	sync.RWMutex
	metadata    []byte             // 元数据
	conf        *Config            // 配置信息
	comm        Comm               // 通信层接口
	mMgr        *membershipManager // 成员管理
	userMsgChan chan *pb.Envelope  // 用户消息通道
	midStore    *LRUCache          // 消息id缓存
	stopping    int32              // 停止服务标志位
	cancel      context.CancelFunc // 用于停止协程运行
}

func New(config *Config) Gossip {
	comm, err := newCommInstanceWithServer(config)
	if err != nil {
		panic(err)
	}

	g := &gossipImpl{
		metadata:    []byte{},
		comm:        comm,
		conf:        config,
		mMgr:        newMembershipManager(config.AliveExpirationTimeout, config.DeadExpirationTimeout),
		userMsgChan: make(chan *pb.Envelope, 1024),
		midStore:    NewCache(config.GossipMidCacheCap, config.GossipMidCacheTimeout),
		stopping:    int32(0),
	}

	log.Infof("node Id: %s", config.Id)

	ctx, cancel := context.WithCancel(context.Background())
	g.cancel = cancel

	go g.dispatch(ctx)
	go g.schedule(ctx)
	go g.handleMessages(ctx)
	go g.handlePresumeDead(ctx)
	go g.connect2BootstrapPeers()

	return g
}

func (g *gossipImpl) UpdateMetadata(metadata []byte) {
	g.Lock()
	defer g.Unlock()
	g.metadata = metadata
}

func (g *gossipImpl) GetMetadata() []byte {
	g.RLock()
	defer g.RUnlock()
	return g.metadata
}

func (g *gossipImpl) GetID() string {
	return g.conf.Id
}

func (g *gossipImpl) GetEndpoint() string {
	return g.conf.Endpoint
}

func (g *gossipImpl) Send(peer *pb.RemotePeer, data []byte) {
	msg := &pb.GossipMessage{
		SrcId: g.GetID(),
		Mid:   g.midStore.NewID(),
		Content: &pb.GossipMessage_P2PMsg{
			P2PMsg: &pb.Envelope{Payload: data},
		},
	}
	g.sendByID(peer.Id, msg)
}

// SendToPeer 点对点发送
func (g *gossipImpl) SendToPeer(peer *pb.RemotePeer, data []byte) ([]byte, error) {
	if g.isStopping() {
		return nil, errors.New("stopping")
	}
	msg := &pb.GossipMessage{
		SrcId: g.GetID(),
		Mid:   g.midStore.NewID(),
		Content: &pb.GossipMessage_P2PMsg{
			P2PMsg: &pb.Envelope{Payload: data},
		},
	}
	return g.comm.SendWithResp(msg, g.conf.ConnTimeout, peer)
}

// Probe 主动探测存活状态
func (g *gossipImpl) Probe(peer *pb.RemotePeer) bool {
	msg := &pb.GossipMessage{
		SrcId: g.GetID(),
		Mid:   g.midStore.NewID(),
		Content: &pb.GossipMessage_Empty{
			Empty: &pb.Empty{},
		},
	}
	_, err := g.comm.SendWithAck(msg, g.conf.ConnTimeout, 1, []*pb.RemotePeer{peer}...)
	return err == nil
}

// dispatch 分发用户数据
func (g *gossipImpl) dispatch(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case env := <-g.userMsgChan:
			d := g.conf.Delegate
			if d != nil {
				d.NotifyMsg(env.Payload)
			}
		}
	}
}

func (g *gossipImpl) schedule(ctx context.Context) {
	if g.conf.GossipInterval > 0 {
		go g.triggerFunc(ctx, g.conf.GossipInterval, g.gossip)
	}

	if g.conf.PullMembershipInterval > 0 {
		go g.triggerFunc(ctx, g.conf.PullMembershipInterval, g.periodicalPullMembership)
	}

	if g.conf.PullInterval > 0 {
		go g.triggerFunc(ctx, g.conf.PullInterval, g.pull)
	}

	if g.conf.AliveTimeInterval > 0 {
		go g.triggerFunc(ctx, g.conf.AliveTimeInterval, g.periodicalSendAlive)
	}

	if g.conf.AliveExpirationCheckInterval > 0 {
		go g.triggerFunc(ctx, g.conf.AliveExpirationCheckInterval, g.periodicalCheckAlive)
	}

	if g.conf.ReconnectInterval > 0 {
		go g.triggerFunc(ctx, g.conf.ReconnectInterval, g.periodicalReconnectToDead)
	}
}

// gossip 广播用户数据
func (g *gossipImpl) gossip() {
	broadcasts := g.conf.Delegate.GetBroadcasts()
	if broadcasts == nil {
		return
	}
	for _, payload := range broadcasts {
		log.Debug("gossip: ", string(payload))
		g.Gossip(&pb.GossipMessage{
			SrcId: g.GetID(),
			Mid:   g.midStore.NewID(),
			Content: &pb.GossipMessage_UserData{
				UserData: &pb.Envelope{
					Payload: payload,
				},
			},
		})
	}
}

// periodicalPullMembership 随机选择节点，然后发送成员信息请求
func (g *gossipImpl) periodicalPullMembership() {
	members := g.mMgr.GetAliveMembers(3, "")
	if len(members) == 0 {
		return
	}
	g.sendMembershipRequest(members[0])
}

// pull (SI Model)定期发送自身状态的摘要信息,远程peer收到该消息后，
// 将回调 LocalState，并将其返回值（状态信息）发送到当前节点，当前节点将回
// 调 MergeRemoteState,完成状态的更新。
// other peer						local peer
//  O   <------------- Summary -------------   O
// /|\  callback state=LocalState(Summary)    /|\
//  |   ------------- State -------------->	  |
// / \										 / \ callback MergeRemoteState(state)
func (g *gossipImpl) pull() {
	d := g.conf.Delegate
	if d == nil {
		return
	}
	digest := d.Summary()

	g.Gossip(&pb.GossipMessage{
		SrcId: g.GetID(),
		Mid:   g.midStore.NewID(),
		Content: &pb.GossipMessage_PullReq{
			PullReq: &pb.Envelope{
				Payload: digest,
			},
		},
	})
}

// periodicalSendAlive 定期发送自身活着的消息
func (g *gossipImpl) periodicalSendAlive() {
	g.Gossip(&pb.GossipMessage{
		SrcId: g.GetID(),
		Mid:   g.midStore.NewID(),
		Content: &pb.GossipMessage_Alive{
			Alive: &pb.AliveMessage{
				Membership: g.Self(),
			},
		},
	})
}

// periodicalCheckAlive 定义检查活着的成员是否过期
func (g *gossipImpl) periodicalCheckAlive() {
	dead := g.mMgr.CheckAliveMembers()
	if len(dead) > 0 {
		log.Debug("dead ", dead)
		deadMembers2Expire := g.mMgr.ExpireDeadMembers(dead)
		d := g.conf.EventDelegate
		for _, member2Expire := range deadMembers2Expire {
			g.comm.CloseConn(member2Expire.Peer())
			if d != nil {
				// 通知离开事件
				d.NotifyLeave(member2Expire)
			}
		}
	}
}

// periodicalReconnectToDead 定期重连
func (g *gossipImpl) periodicalReconnectToDead() {
	// 检查并删除重连超时的节点
	expiredMembers := g.mMgr.CheckExpirationMember()
	for _, member := range expiredMembers {
		// 广播该成员离开集群的消息（LeaveMessage）
		g.Gossip(&pb.GossipMessage{
			SrcId: g.GetID(),
			Mid:   g.midStore.NewID(),
			Content: &pb.GossipMessage_Leave{
				Leave: &pb.LeaveMessage{
					Membership: member,
				},
			},
		})
	}

	wg := &sync.WaitGroup{}
	for _, member := range g.mMgr.DeadToSlice() {
		wg.Add(1)
		go func(member pb.Membership) {
			defer wg.Done()
			if g.Ping(member.Peer()) {
				g.sendMembershipRequest(member)
			}
		}(member)
	}
	wg.Wait()
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
	for !g.isStopping() {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			f()
		}
	}
}

func (g *gossipImpl) handlePresumeDead(ctx context.Context) {
	for !g.isStopping() {
		select {
		case id := <-g.comm.PresumeDead():
			if g.mMgr.IsAlive(id) {
				expiredMembers := g.mMgr.ExpireDeadMembers([]string{id})
				d := g.conf.EventDelegate
				if d == nil {
					break
				}
				for _, m := range expiredMembers {
					d.NotifyLeave(m)
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func (g *gossipImpl) sendMembershipRequest(member pb.Membership) {
	msg := &pb.GossipMessage{
		SrcId: g.GetID(),
		Mid:   g.midStore.NewID(),
		Content: &pb.GossipMessage_MemReq{
			MemReq: &pb.MembershipRequest{
				Membership: g.Self(),
			},
		},
	}
	g.send(member, msg)
}

func (g *gossipImpl) handleMessages(ctx context.Context) {
	in := g.comm.Accept()
	for !g.isStopping() {
		select {
		case <-ctx.Done():
			return
		case m := <-in:
			g.handleMsgFromComm(m)
		}
	}
}

func (g *gossipImpl) handleMsgFromComm(msg *ReceivedMessage) {
	if msg == nil {
		return
	}

	srcId := msg.SrcId

	if srcId == g.GetID() {
		// 自身消息
		return
	}

	// 最新通话时间
	g.mMgr.Alive(srcId)
	defer g.mMgr.Alive(srcId)

	// 检查是否是已读消息
	if g.midStore.Has(msg.Mid) {
		msg.Ack(g.GetID(), ErrAlreadyReceived)
		return
	}
	g.midStore.Set(msg.Mid)

	// 回复确认消息
	msg.Ack(g.GetID(), nil)

	switch msg.GetContent().(type) {
	case *pb.GossipMessage_Conn:
		g.handleConnEstablishMessage(msg)
	case *pb.GossipMessage_Alive:
		g.handleAliveMessage(msg)
		go g.Forward(*msg)
	case *pb.GossipMessage_Leave:
		g.handleLeaveMessage(msg)
		go g.Forward(*msg)
	case *pb.GossipMessage_Empty:
		// 不做处理，因为上面已经发了确认消息
	case *pb.GossipMessage_UserData:
		g.handleUserData(msg)
		go g.Forward(*msg)
	case *pb.GossipMessage_PullReq:
		g.handlePullRequest(msg)
	case *pb.GossipMessage_PullResp:
		g.handlePullResponse(msg)
	case *pb.GossipMessage_MemReq:
		g.handleMembershipRequest(msg)
	case *pb.GossipMessage_MemResp:
		g.handleMembershipResponse(msg)
	case *pb.GossipMessage_P2PMsg:
		// 点对点消息不做转发
		g.userMsgChan <- msg.GetP2PMsg()
	}
}

func (g *gossipImpl) handleLeaveMessage(msg *ReceivedMessage) {
	log.Debugf("[begin]handle leave message, msg:%v", *msg)
	defer log.Debug("[end]handle leave message")

	member := msg.GetLeave().Membership

	m, exist := g.mMgr.Load(member.Id)
	if !exist {
		return
	}

	if g.mMgr.IsAlive(member.Id) {
		g.comm.CloseConn(&pb.RemotePeer{Id: member.Id})
		d := g.conf.EventDelegate
		if d != nil {
			d.NotifyLeave(*m)
		}
	}

	g.mMgr.Delete(member.Id)
}

func (g *gossipImpl) handleConnEstablishMessage(msg *ReceivedMessage) {
	log.Debugf("[begin]handle conn establish message, msg:%v", *msg)
	defer log.Debug("[end]handle conn establish message")

	msg.Response(&pb.GossipMessage{
		SrcId: g.GetID(),
		Mid:   msg.Mid,
		Content: &pb.GossipMessage_Conn{
			Conn: &pb.ConnEstablish{
				Id: g.GetID(),
			},
		},
	})
}

func (g *gossipImpl) handleUserData(msg *ReceivedMessage) {
	log.Debugf("[begin]handle user data, msg:%v", *msg)
	defer log.Debug("[end]handle user data")

	g.userMsgChan <- msg.GetUserData()
}

func (g *gossipImpl) handlePullResponse(msg *ReceivedMessage) {
	log.Debugf("[begin]handle pull response, msg:%v", *msg)
	defer log.Debug("[end]handle pull response")

	d := g.conf.Delegate
	if d == nil {
		return
	}
	d.MergeRemoteState(msg.GetPullResp().Payload)
}

func (g *gossipImpl) handlePullRequest(msg *ReceivedMessage) {
	log.Debugf("[begin]handle pull request, msg:%v", *msg)
	defer log.Debug("[end]handle pull request")

	var state []byte
	d := g.conf.Delegate
	if d != nil {
		state = d.LocalState(msg.GetPullReq().Payload)
	}

	// send pull response
	go msg.Response(&pb.GossipMessage{
		SrcId: g.GetID(),
		Mid:   g.midStore.NewID(),
		Content: &pb.GossipMessage_PullResp{
			PullResp: &pb.Envelope{
				Payload: state,
			},
		},
	})
}

func (g *gossipImpl) handleMembershipResponse(msg *ReceivedMessage) {
	log.Debugf("[begin]handle membership response, msg:%v", *msg)
	defer log.Debug("[end]handle membership response")

	// 更新来源节点
	g.handleAliveMember(msg.GetMemResp().Membership)

	// 更新成员信息
	for _, m := range msg.GetMemResp().Alive {
		if m.Id == g.GetID() {
			continue
		}
		g.handleAliveMember(m)
	}
	for _, m := range msg.GetMemResp().Dead {
		if m.Id == g.GetID() {
			continue
		}
		if _, known := g.mMgr.Load(m.Id); known {
			continue
		}
		g.mMgr.LearnNewMembers([]pb.Membership{}, []pb.Membership{m})
	}
}

func (g *gossipImpl) handleAliveMessage(msg *ReceivedMessage) {
	log.Debugf("[begin]handle alive message, msg:%v", *msg)
	defer log.Debug("[end]handle alive message")

	g.handleAliveMember(msg.GetAlive().Membership)
}

func (g *gossipImpl) handleMembershipRequest(msg *ReceivedMessage) {
	log.Debugf("[begin]handle membership request, msg:%v", *msg)
	defer log.Debug("[end]handle membership request")

	g.handleAliveMember(msg.GetMemReq().Membership)

	msg.Response(&pb.GossipMessage{
		SrcId: g.GetID(),
		Mid:   g.midStore.NewID(),
		Content: &pb.GossipMessage_MemResp{
			MemResp: &pb.MembershipResponse{
				Membership: g.Self(),
				Alive:      g.mMgr.AliveToSlice(),
				Dead:       g.mMgr.DeadToSlice(),
			},
		},
	})
}

func (g *gossipImpl) handleAliveMember(member pb.Membership) {
	// 是否是自身发出的消息
	if member.Id == g.GetID() {
		return
	}

	d := g.conf.EventDelegate

	// 是否是已知成员
	oldMember, known := g.mMgr.Load(member.Id)
	if !known {
		m := pb.Membership{Id: member.Id, Metadata: member.Metadata, Endpoint: member.Endpoint}
		g.mMgr.LearnNewMembers([]pb.Membership{m}, []pb.Membership{})
		if d != nil {
			d.NotifyJoin(m)
		}
		return
	}

	if d == nil {
		// 复活成员或更新成员
		g.mMgr.ResurrectMember(&member)
		return
	}

	// d != nil

	if g.mMgr.IsAlive(member.Id) {
		// 判断元数据是否更新
		if !bytes.Equal(oldMember.Metadata, member.Metadata) {
			d.NotifyUpdate(member)
		}
	} else {
		// dead -> alive
		d.NotifyJoin(member)
	}

	g.mMgr.ResurrectMember(&member)
}

func (g *gossipImpl) Ping(peer *pb.RemotePeer) bool {
	log.Debugf("[begin]ping, peer:%v", *peer)
	defer log.Debug("[end]ping")

	err := g.comm.Probe(peer)
	return err == nil
}

func (g *gossipImpl) send(member pb.Membership, msg *pb.GossipMessage) {
	log.Debugf("[begin]send, peer:%v msg:%v", member.Peer(), *msg)
	defer log.Debug("[end]send")
	if g.isStopping() {
		return
	}
	g.comm.Send(msg, member.Peer())
}

func (g *gossipImpl) sendByID(id string, msg *pb.GossipMessage) {
	log.Debugf("[begin]send by id, id:%v msg:%v", id, *msg)
	defer log.Debug("[end]send by id")

	if g.isStopping() {
		return
	}

	member, exist := g.mMgr.Load(id)
	if !exist {
		log.Warnf("not exist membership, id:%s", id)
		return
	}

	g.comm.Send(msg, member.Peer())
}

func (g *gossipImpl) Forward(msg ReceivedMessage) {
	if g.isStopping() {
		return
	}

	log.Debugf("[begin]forward, msg:%v", msg)
	defer log.Debug("[end]forward")

	members := g.mMgr.GetAliveMembers(g.conf.GossipNodes, msg.SrcId)
	msg.SrcId = g.GetID() // 切换来源标识
	for _, member := range members {
		g.sendByID(member.Id, msg.GossipMessage)
	}
}

func (g *gossipImpl) Gossip(msg *pb.GossipMessage) {
	if g.isStopping() {
		return
	}

	//log.Debugf("[begin]gossip, msg:%v", msg)
	//defer log.Debug("[end]gossip")

	if g.conf.PropagateIterations == 0 {
		return
	}

	// 推送次数
	k := g.conf.PropagateIterations

	for i := 0; i < k; i++ {
		members := g.mMgr.GetAliveMembers(g.conf.GossipNodes, "")
		for _, member := range members {
			g.sendByID(member.Id, msg)
		}
	}
}

func (g *gossipImpl) Stop() {
	log.Debug("[begin]stopping")
	defer log.Debug("[end]stopped")

	ok := atomic.CompareAndSwapInt32(&g.stopping, 0, 1)
	if !ok {
		return
	}
	g.Gossip(&pb.GossipMessage{
		SrcId: g.GetID(),
		Mid:   g.midStore.NewID(),
		Content: &pb.GossipMessage_Leave{
			Leave: &pb.LeaveMessage{
				Membership: g.Self(),
			},
		},
	})
	g.cancel()
	g.comm.Stop()
}

func (g *gossipImpl) isStopping() bool {
	return atomic.LoadInt32(&g.stopping) == int32(1)
}

func (g *gossipImpl) connect2BootstrapPeers() {
	for _, endpoint := range g.conf.BootstrapPeers {
		g.connect(endpoint)
	}
}

func (g *gossipImpl) connect(endpoint string) {
	go func() {
		for i := 0; i < g.conf.MaxConnectionAttempts && !g.isStopping(); i++ {
			p, err := g.comm.Handshake(endpoint)
			if err != nil {
				if g.isStopping() {
					return
				}
				time.Sleep(g.conf.ReconnectInterval)
				continue
			}

			// 握手成功
			log.Debugf("handshake success %s %s", p.Endpoint, p.Id)

			msg := &pb.GossipMessage{
				SrcId: g.GetID(),
				Mid:   g.midStore.NewID(),
				Content: &pb.GossipMessage_MemReq{
					MemReq: &pb.MembershipRequest{
						Membership: g.Self(),
					},
				},
			}

			for j := 0; j < g.conf.MaxConnectionAttempts && !g.isStopping(); j++ {
				err = g.SendUntilAcked(p, msg)
				if err != nil {
					time.Sleep(g.conf.ReconnectInterval)
					continue
				}
				log.Infof("connect %s success", endpoint)
				break
			}

			return
		}
	}()
}

func (g *gossipImpl) SendUntilAcked(p *pb.RemotePeer, msg *pb.GossipMessage) error {
	log.Debugf("[begin]send until acked, peer:%v msg:%v", *p, msg)
	defer log.Debug("[end]send until acked")

	_, err := g.comm.SendWithAck(msg, g.conf.ConnTimeout, 1, []*pb.RemotePeer{p}...)
	return err
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

func (g *gossipImpl) Self() pb.Membership {
	return pb.Membership{
		Id:       g.GetID(),
		Metadata: g.GetMetadata(),
		Endpoint: g.GetEndpoint(),
	}
}
