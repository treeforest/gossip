package gossip

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/treeforest/gossip/pb"
	log "github.com/treeforest/logger"
	"google.golang.org/grpc"
	"sync"
)

type connFactory interface {
	createConnection(endpoint string, id string) (*connection, error)
}

type connectionStore struct {
	sync.RWMutex
	l            log.Logger
	isClosing    bool
	connFactory  connFactory
	id2Conn      map[string]*connection
	shutdownOnce sync.Once
}

func newConnStore(logLevel log.Level, connFactory connFactory) *connectionStore {
	return &connectionStore{
		l:           log.NewStdLogger(log.WithPrefix("connStore"), log.WithLevel(logLevel)),
		connFactory: connFactory,
		isClosing:   false,
		id2Conn:     make(map[string]*connection),
	}
}

func (cs *connectionStore) getConnection(peer *pb.RemotePeer) (*connection, error) {
	cs.RLock()
	isClosing := cs.isClosing
	cs.RUnlock()

	if isClosing {
		return nil, errors.New("shutting down")
	}

	id := peer.Id
	endpoint := peer.Endpoint

	cs.RLock()
	conn, exists := cs.id2Conn[id]
	if exists {
		cs.RUnlock()
		return conn, nil
	}
	cs.RUnlock()

	createdConnection, err := cs.connFactory.createConnection(endpoint, id)
	if err == nil {
		cs.l.Debugf("create new connection to %s, %s", endpoint, id)
	}

	cs.RLock()
	isClosing = cs.isClosing
	cs.RUnlock()
	if isClosing {
		return nil, fmt.Errorf("connStore is closing")
	}

	cs.Lock()
	defer cs.Unlock()

	// 再次检查，可能在连接期间有其它协程连接到了远程peer
	conn, exists = cs.id2Conn[id]
	if exists {
		if createdConnection != nil {
			cs.l.Debugf("close old connection %s %s", endpoint, id)
			createdConnection.close()
		}
		return conn, nil
	}

	// 不存在连接，且连接失败
	if err != nil {
		return nil, errors.WithStack(err)
	}

	conn = createdConnection
	cs.id2Conn[conn.id] = conn
	//cs.l.Debugf("store connection %s %s", conn.peer.Endpoint, conn.peer.Id)

	go conn.serviceConnection()

	return conn, nil
}

func (cs *connectionStore) connNum() int {
	cs.RLock()
	defer cs.RUnlock()
	return len(cs.id2Conn)
}

func (cs *connectionStore) shutdown() {
	cs.shutdownOnce.Do(func() {
		cs.l.Debug("shutdown")
		cs.Lock()
		cs.isClosing = true

		for _, conn := range cs.id2Conn {
			conn.close()
		}
		cs.id2Conn = make(map[string]*connection)

		cs.Unlock()
	})
}

func (cs *connectionStore) closeByID(id string) {
	cs.Lock()
	defer cs.Unlock()
	if conn, exists := cs.id2Conn[id]; exists {
		//cs.l.Debugf("close connection by id(%s) %s", id, conn.peer.Endpoint)
		conn.close()
		delete(cs.id2Conn, conn.id)
	}
}

func (cs *connectionStore) onConnected(cc *grpc.ClientConn, stream Stream, peer *pb.RemotePeer) *connection {
	cs.Lock()
	defer cs.Unlock()

	if c, exists := cs.id2Conn[peer.Id]; exists {
		cs.l.Debugf("close old connection %s %s", peer.Id, peer.Endpoint)
		c.close()
	}

	conn := newConnection(cc, stream)
	conn.id = peer.Id
	cs.id2Conn[conn.id] = conn
	return conn
}
