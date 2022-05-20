package gossip

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/treeforest/gossip/pb"
	log "github.com/treeforest/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

type Comm interface {
	// GetID 返回此实例的 Id
	GetID() string

	// Send 发送消息到远程peer
	Send(msg *pb.GossipMessage, peers ...*pb.RemotePeer)

	// SendWithResp 发送消息到远程peer，并等待响应或超时到来
	SendWithResp(msg *pb.GossipMessage, timeout time.Duration, p *pb.RemotePeer) ([]byte, error)

	// SendWithAck 发送消息到远程peer，等待来自至少minAck数量的确认，或直到某个超时时间
	SendWithAck(msg *pb.GossipMessage, timeout time.Duration, minAck int, peers ...*pb.RemotePeer) (ackCount int, err error)

	// Probe 探测一个远程peer，如果它回应，则返回nil；否则，则出错
	Probe(p *pb.RemotePeer) error

	// Handshake 握手，并获取远程peer的id
	Handshake(endpoint string) (*pb.RemotePeer, error)

	// PresumeDead 返回疑似离线的节点端点的只读通道
	PresumeDead() <-chan string

	// CloseConn 关闭某个端点的连接
	CloseConn(p *pb.RemotePeer)

	// Stop 停止模块
	Stop()

	// Accept 返回消息的可读通道
	Accept() <-chan *ReceivedMessage
}

func newCommInstanceWithServer(config *Config) (Comm, error) {
	if config.Port <= 0 {
		panic("port less 0")
	}

	listenAddress := fmt.Sprintf("0.0.0.0:%d", config.Port)
	ll, err := net.Listen("tcp", listenAddress)
	if err != nil {
		panic(err)
	}

	s := grpc.NewServer(config.ServerOptions...)

	commInst := &commImpl{
		l:             log.NewStdLogger(log.WithPrefix("comm"), log.WithLevel(config.LogLevel)),
		config:        config,
		deadEndpoints: make(chan string, 128),
		lock:          &sync.Mutex{},
		lsnr:          ll,
		gSrv:          s,
		exitChan:      make(chan struct{}, 1),
		stopWG:        sync.WaitGroup{},
		stopping:      int32(0),
		pubSub:        NewPubSub(),
		msgs:          make(chan *ReceivedMessage, 2048),
	}
	commInst.connStore = newConnStore(config.LogLevel, commInst)

	commInst.stopWG.Add(1)
	pb.RegisterGossipServer(s, commInst)
	go func() {
		defer commInst.stopWG.Done()
		_ = s.Serve(ll)
	}()

	return commInst, nil
}

type commImpl struct {
	l             log.Logger
	config        *Config
	connStore     *connectionStore
	deadEndpoints chan string
	lock          *sync.Mutex
	lsnr          net.Listener
	gSrv          *grpc.Server
	exitChan      chan struct{}
	stopWG        sync.WaitGroup
	stopping      int32
	pubSub        *PubSub
	msgs          chan *ReceivedMessage
}

type ReceivedMessage struct {
	*pb.GossipMessage
	lock sync.Locker
	conn *connection
}

// Response 响应消息
func (m *ReceivedMessage) Response(msg *pb.GossipMessage) {
	m.conn.send(msg, func(e error) {
		log.Errorf("response failed: %+v", e)
	})
}

// Ack 确认消息
func (m *ReceivedMessage) Ack(id string, err error) {
	ackMsg := &pb.GossipMessage{
		SrcId: id,
		Mid:   m.Mid,
		Content: &pb.GossipMessage_Ack{
			Ack: &pb.Acknowledgement{},
		},
	}
	if err != nil {
		ackMsg.GetAck().Error = err.Error()
	}
	m.Response(ackMsg)
}

func (c *commImpl) createConnection(endpoint string, expectedID string) (*connection, error) {
	if c.isStopping() {
		return nil, errors.New("stopping")
	}

	c.l.Debugf("[begin]create connection, endpoint:%s expectedID:%s", endpoint, expectedID)
	defer c.l.Debug("[end]create connection")

	cc, err := c.dial(endpoint)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	gossipClient := pb.NewGossipClient(cc)

	if err = c.ping(gossipClient, c.config.ConnTimeout); err != nil {
		_ = cc.Close()
		return nil, errors.WithStack(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var stream pb.Gossip_GossipStreamClient
	if stream, err = gossipClient.GossipStream(ctx); err == nil {
		var p *pb.RemotePeer
		p, err = c.connectRemotePeer(stream)
		if err == nil {
			// 当不知道远程peer的id时，expectedID为空
			if expectedID != "" && p.Id != expectedID {
				cancel()
				_ = cc.Close()
				return nil, errors.Errorf("auth id failed, expect:%s actual:%s", expectedID, p.Id)
			}
			conn := newConnection(cc, stream)
			conn.id = p.Id
			//conn.peer = p
			conn.cancel = cancel

			h := func(m *pb.GossipMessage) {
				c.msgs <- &ReceivedMessage{
					GossipMessage: m,
					conn:          conn,
					lock:          conn,
				}
			}
			conn.handler = interceptAcks(h, p.Id, c.pubSub)
			return conn, nil
		}
	}
	cancel()
	_ = cc.Close()
	return nil, errors.WithStack(err)
}

func (c *commImpl) dial(endpoint string) (*grpc.ClientConn, error) {
	var dialOpts []grpc.DialOption
	var ctx context.Context
	var cancel context.CancelFunc
	dialOpts = append(dialOpts, grpc.WithBlock())
	dialOpts = append(dialOpts, c.config.DialOptions...)
	ctx, cancel = context.WithTimeout(context.Background(), c.config.DialTimeout)
	defer cancel()
	return grpc.DialContext(ctx, endpoint, dialOpts...)
}

func (c *commImpl) ping(client pb.GossipClient, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err := client.Ping(ctx, &pb.Empty{})
	return err
}

func (c *commImpl) GetID() string {
	return c.config.Id
}

func (c *commImpl) Send(msg *pb.GossipMessage, peers ...*pb.RemotePeer) {
	c.l.Debugf("[begin]send, peers:%v msg:%v", peers, *msg)
	defer c.l.Debug("[end]send")

	if c.isStopping() || len(peers) == 0 {
		return
	}

	for _, p := range peers {
		go func(p *pb.RemotePeer, msg *pb.GossipMessage) {
			c.sendToEndpoint(p, msg)
		}(p, msg)
	}
}

func (c *commImpl) SendWithResp(msg *pb.GossipMessage, timeout time.Duration, p *pb.RemotePeer) ([]byte, error) {
	c.l.Debugf("[begin]send with response, peer:%v msg:%v", *p, *msg)
	defer c.l.Debug("[end]send with response")

	if p == nil {
		return nil, errors.New("peer is nil")
	}

	if c.isStopping() {
		return nil, errors.New("stopping")
	}

	topic := topicForAck(msg.Mid, p.Id)
	sub := c.pubSub.Subscribe(topic, timeout)

	c.sendToEndpoint(p, msg)

	m, err := sub.Listen()
	if err != nil {
		return nil, err
	}

	return m.(*pb.Envelope).Payload, nil
}

func (c *commImpl) SendWithAck(msg *pb.GossipMessage, timeout time.Duration, minAck int, peers ...*pb.RemotePeer) (
	ackCount int, err error) {
	c.l.Debug("[begin]send with ack, peers:%v msg:%v", peers, *msg)
	defer c.l.Debug("[end]send with ack")

	if len(peers) == 0 {
		return 0, nil
	}

	if c.isStopping() {
		return 0, errors.New("stopping")
	}

	acks := make(chan error, len(peers))

	for _, p := range peers {
		topic := topicForAck(msg.Mid, p.Id)
		sub := c.pubSub.Subscribe(topic, timeout)
		go func(p *pb.RemotePeer) {
			c.sendToEndpoint(p, msg)
			var m interface{}
			m, err = sub.Listen()
			if err != nil {
				acks <- err
				return
			}
			ackMsg, ok := m.(*pb.Acknowledgement)
			if !ok {
				acks <- errors.New("not ack msg")
				return
			}
			if ackMsg.Error != "" && ackMsg.Error != ErrAlreadyReceived.Error() {
				acks <- errors.New(ackMsg.Error)
			} else {
				acks <- nil
			}
		}(p)
	}

	ackCount = 0
	count := 0
	for {
		e := <-acks
		count++
		if e == nil {
			ackCount++
		}
		if ackCount == minAck {
			err = nil
			break
		}
		if count == len(peers) {
			err = errors.New("ackCount is less minAck")
			break
		}
	}
	return
}

func (c *commImpl) Probe(p *pb.RemotePeer) error {
	if c.isStopping() {
		return errors.New("stopping")
	}

	c.l.Debugf("[begin]probe, peer:%v", *p)
	defer c.l.Debug("[end]probe")

	cc, err := c.dial(p.Endpoint)
	if err != nil {
		return err
	}
	defer cc.Close()
	err = c.ping(pb.NewGossipClient(cc), c.config.ConnTimeout)
	return err
}

func (c *commImpl) Handshake(endpoint string) (*pb.RemotePeer, error) {
	if c.isStopping() {
		return nil, errors.New("stopping")
	}

	c.l.Debug("[begin]handshake, endpoint:%s", endpoint)
	defer c.l.Debug("[end]handshake")

	cc, err := c.dial(endpoint)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer cc.Close()

	cl := pb.NewGossipClient(cc)
	err = c.ping(cl, c.config.HandshakeTimeout)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	var stream pb.Gossip_GossipStreamClient
	var p *pb.RemotePeer

	stream, err = cl.GossipStream(context.Background())
	if err != nil {
		return nil, errors.WithStack(err)
	}

	p, err = c.connectRemotePeer(stream)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if p.Id == c.GetID() {
		return nil, errors.New("id conflict")
	}

	return p, nil
}

func (c *commImpl) PresumeDead() <-chan string {
	return c.deadEndpoints
}

func (c *commImpl) CloseConn(p *pb.RemotePeer) {
	c.l.Debugf("[begin]close conn, endpoint:%s", p.Endpoint)
	defer c.l.Debug("[end]close conn")
	c.connStore.closeByID(p.Id)
}

func (c *commImpl) Stop() {
	if c.isStopping() {
		return
	}

	c.l.Debug("comm stopping...")
	defer c.l.Debug("comm stopped")

	atomic.StoreInt32(&c.stopping, int32(1))
	if c.gSrv != nil {
		c.gSrv.Stop()
	}
	if c.lsnr != nil {
		_ = c.lsnr.Close()
	}
	c.connStore.shutdown()
	c.exitChan <- struct{}{}
	c.stopWG.Wait()
}

func (c *commImpl) sendToEndpoint(p *pb.RemotePeer, msg *pb.GossipMessage) {
	if c.isStopping() {
		return
	}

	c.l.Debugf("[begin]send to endpoint, peer:%v msg:%v", *p, *msg)
	defer c.l.Debugf("[end]send to endpoint")

	var err error

	conn, err := c.connStore.getConnection(p)
	if err != nil {
		c.l.Debugf("failed to get connection:%s %+v", p.Endpoint, err)
		c.disconnect(p.Id)
		return
	}

	conn.send(msg, func(err error) {
		c.l.Debug("send failed, begin disconnect (peer:%v msg:%v error:%+v)", *p, *msg, err)
		c.disconnect(p.Id)
	})
}

func (c *commImpl) disconnect(id string) {
	if c.isStopping() {
		return
	}
	c.l.Debugf("disconnect %s", id)
	c.deadEndpoints <- id
	c.connStore.closeByID(id)
}

func extractRemoteAddress(stream Stream) string {
	var remoteAddress string
	p, ok := peer.FromContext(stream.Context())
	if ok {
		if address := p.Addr; address != nil {
			remoteAddress = address.String()
		}
	}
	return remoteAddress
}

func readWithTimeout(stream interface{}, timeout time.Duration, address string) (*pb.GossipMessage, error) {
	incChan := make(chan *pb.GossipMessage, 1)
	errChan := make(chan error, 1)
	go func() {
		if srvStr, isServerStr := stream.(pb.Gossip_GossipStreamServer); isServerStr {
			if msg, err := srvStr.Recv(); err == nil {
				incChan <- msg
			}
		} else if clStr, isClientStr := stream.(pb.Gossip_GossipStreamClient); isClientStr {
			if msg, err := clStr.Recv(); err == nil {
				incChan <- msg
			}
		} else {
			panic(errors.Errorf("Stream isn't a GossipStreamServer or a GossipStreamClient, but %v. Aborting", reflect.TypeOf(stream)))
		}
	}()
	select {
	case <-time.NewTicker(timeout).C:
		return nil, errors.Errorf("Timed out waiting for connection message from %s", address)
	case m := <-incChan:
		return m, nil
	case err := <-errChan:
		return nil, errors.WithStack(err)
	}
}

func (c *commImpl) connectRemotePeer(stream Stream) (*pb.RemotePeer, error) {
	remoteAddress := extractRemoteAddress(stream)
	msg := &pb.GossipMessage{
		SrcId: c.GetID(),
		Mid:   uuid.New().String(),
		Content: &pb.GossipMessage_Conn{
			Conn: &pb.ConnEstablish{
				Id: c.GetID(),
			},
		},
	}

	if err := stream.Send(msg); err != nil {
		return nil, errors.WithStack(err)
	}

	msg, err := readWithTimeout(stream, c.config.ConnTimeout, remoteAddress)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	resp := msg.GetConn()
	if resp == nil {
		return nil, errors.New("wrong type")
	}

	if resp.Id == "" {
		return nil, errors.New("no Id")
	}

	return &pb.RemotePeer{Id: resp.Id, Endpoint: remoteAddress}, nil
}

func (c *commImpl) isStopping() bool {
	return atomic.LoadInt32(&c.stopping) == int32(1)
}

func (c *commImpl) GossipStream(stream pb.Gossip_GossipStreamServer) error {
	if c.isStopping() {
		return errors.New("shopping")
	}

	c.l.Debug("[begin] GossipStream")
	defer c.l.Debug("[end] GossipStream")

	c.l.Debug("connect remote peer...")
	remotePeer, err := c.connectRemotePeer(stream)
	if err != nil {
		return err
	}

	c.l.Debugf("on connected, peer:", *remotePeer)
	conn := c.connStore.onConnected(nil, stream, remotePeer)

	if conn == nil {
		return nil
	}

	h := func(m *pb.GossipMessage) {
		c.msgs <- &ReceivedMessage{
			GossipMessage: m,
			conn:          conn,
			lock:          conn,
		}
	}

	conn.handler = interceptAcks(h, conn.id, c.pubSub)

	defer func() {
		c.connStore.closeByID(remotePeer.Id)
		conn.close()
		c.l.Debugf("close connection, peer:%v", remotePeer)
	}()

	c.l.Debugf("service connection, endpoint:%s", remotePeer.Endpoint)
	return conn.serviceConnection()
}

func (c *commImpl) Ping(ctx context.Context, req *pb.Empty) (*pb.Empty, error) {
	return &pb.Empty{}, nil
}

func (c *commImpl) Accept() <-chan *ReceivedMessage {
	return c.msgs
}

func topicForAck(mid, id string) string {
	return fmt.Sprintf("%s_%s", mid, id)
}

func interceptAcks(nextHandler func(msg *pb.GossipMessage), id string, pubSub *PubSub) func(msg *pb.GossipMessage) {
	return func(msg *pb.GossipMessage) {
		if msg.GetAck() != nil {
			topic := topicForAck(msg.Mid, id)
			_ = pubSub.Publish(topic, msg.GetAck())
			return
		}
		if msg.GetP2PMsg() != nil {
			topic := topicForAck(msg.Mid, id)
			_ = pubSub.Publish(topic, msg.GetP2PMsg())
			return
		}
		nextHandler(msg)
	}
}
