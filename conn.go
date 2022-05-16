package gossip

import (
	"context"
	"github.com/pkg/errors"
	"github.com/treeforest/gossip/pb"
	"google.golang.org/grpc"
	"sync"
)

type msgSending struct {
	msg   *pb.GossipMessage
	onErr func(error)
}

func newConnection(c *grpc.ClientConn, stream Stream) *connection {
	return &connection{
		cancel:       nil,
		outBuff:      make(chan *msgSending, 256),
		conn:         c,
		gossipStream: stream,
		stopChan:     make(chan struct{}, 1),
	}
}

type connection struct {
	sync.RWMutex
	cancel       context.CancelFunc
	outBuff      chan *msgSending        // 发送通道
	id           string                  // id
	handler      func(*pb.GossipMessage) // 消息回调函数
	conn         *grpc.ClientConn        // grpc客户端连接
	gossipStream Stream                  // 流对象
	stopChan     chan struct{}
	stopOnce     sync.Once
}

func (conn *connection) close() {
	conn.stopOnce.Do(func() {
		close(conn.stopChan)

		if conn.conn != nil {
			conn.conn.Close()
		}

		if conn.cancel != nil {
			conn.cancel()
		}
	})
}

func (conn *connection) send(msg *pb.GossipMessage, onErr func(error)) {
	m := &msgSending{
		msg:   msg,
		onErr: onErr,
	}

	select {
	case conn.outBuff <- m:
	case <-conn.stopChan:
	}
}

func (conn *connection) serviceConnection() error {
	errChan := make(chan error, 1)
	msgChan := make(chan *pb.GossipMessage, 256)
	defer close(msgChan)

	go conn.readFromStream(errChan, msgChan)

	go conn.writeToStream()

	for {
		select {
		case <-conn.stopChan:
			//log.Debug("closing reading from ", conn.peer.Endpoint)
			return nil
		case err := <-errChan:
			return err
		case msg := <-msgChan:
			conn.handler(msg)
		}
	}
}

func (conn *connection) writeToStream() {
	stream := conn.gossipStream
	for {
		select {
		case m := <-conn.outBuff:
			err := stream.Send(m.msg)
			if err != nil {
				go m.onErr(errors.WithStack(err))
				return
			}
		case <-conn.stopChan:
			// log.Debug("closing writing to Stream")
			return
		}
	}
}

func (conn *connection) readFromStream(errChan chan error, msgChan chan *pb.GossipMessage) {
	defer func() {
		recover()
	}() // msgsCh might be closed

	stream := conn.gossipStream

	for {
		select {
		case <-conn.stopChan:
			return
		default:
			msg, err := stream.Recv()
			if err != nil {
				errChan <- err
				return
			}
			select {
			case msgChan <- msg:
			case <-conn.stopChan:
				return
			}
		}
	}
}

type Stream interface {
	Send(msg *pb.GossipMessage) error
	Recv() (*pb.GossipMessage, error)
	grpc.Stream
}
