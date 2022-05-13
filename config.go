package gossip

import (
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"time"
)

type Config struct {
	// Id 节点id
	Id string

	// Port 监听端口
	Port int

	// Endpoint 暴露的地址
	Endpoint string

	// DialTimeout 拨号超时时间
	DialTimeout time.Duration

	// DialOptions 拨号参数设置
	DialOptions []grpc.DialOption

	// ServerOptions 服务端参数设置
	ServerOptions []grpc.ServerOption

	// Delegate 消息代理接口（用户必须实现）
	Delegate Delegate

	// EventDelegate 事件代理（用于通知节点上下线）
	EventDelegate EventDelegate

	// GossipInterval 广播消息的间隔时间
	GossipInterval time.Duration

	// GossipNodes 随机选取多少个节点进行发送,-1则表示发送给所有的邻居节点
	GossipNodes int

	// GossipMidCacheCap 对于广播消息id缓存的最大容量
	GossipMidCacheCap int

	// GossipMidCacheTimeout 广播消息id缓存的超时时间，超时后将会
	// 将消息id从缓存中删除
	GossipMidCacheTimeout time.Duration

	// PullMembershipInterval 成员同步间隔时间
	PullMembershipInterval time.Duration

	// HandshakeTimeout 握手超时时间
	HandshakeTimeout time.Duration

	// ConnTimeout 连接超时时间
	ConnTimeout time.Duration

	// AliveTimeInterval 主动发送心跳包的间隔时间
	AliveTimeInterval time.Duration

	// AliveExpirationTimeout 存活节点的超时时间，若在超时时间内没有收到
	// 它的心跳包，则默认其失效，将从alive转换到dead。
	AliveExpirationTimeout time.Duration

	// DeadExpirationTimeout 失效节点到移除的超时时间，若在超时时间内没有
	// 重连成功，则移除该节点的信息，并广播到集群中
	DeadExpirationTimeout time.Duration

	// AliveExpirationCheckInterval 检查存活节点是否超时的时间间隔
	AliveExpirationCheckInterval time.Duration

	// ReconnectInterval 重连的时间间隔
	ReconnectInterval time.Duration

	// MaxConnectionAttempts 最大的连接次数
	MaxConnectionAttempts int

	// PullInterval 拉取远端peer状态的间隔时间
	PullInterval time.Duration

	// PropagateIterations gossip消息的推送次数
	PropagateIterations int

	// BootstrapPeers 启动时连接的节点
	BootstrapPeers []string
}

func DefaultConfig() *Config {
	return &Config{
		Id:          uuid.NewString(),
		Port:        4399,
		Endpoint:    "localhost:4399",
		DialTimeout: time.Second * 3,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
		ServerOptions: []grpc.ServerOption{
			grpc.Creds(insecure.NewCredentials()),
		},
		Delegate:                     nil,
		EventDelegate:                nil,
		GossipInterval:               time.Millisecond * 100,
		GossipNodes:                  6,
		GossipMidCacheCap:            1024 * 1024 * 4,
		GossipMidCacheTimeout:        time.Second * 30,
		PullMembershipInterval:       time.Second * 4,
		HandshakeTimeout:             time.Second * 5,
		ConnTimeout:                  time.Second * 2,
		AliveTimeInterval:            time.Second * 4,
		AliveExpirationTimeout:       time.Second * 40,
		AliveExpirationCheckInterval: time.Second * 2,
		ReconnectInterval:            time.Second * 2,
		MaxConnectionAttempts:        120, // about 4min
		PullInterval:                 time.Second * 3,
		PropagateIterations:          1,
	}
}

func (c *Config) SetDialOption(opts ...grpc.DialOption) {
	c.DialOptions = opts
}

func (c *Config) SetServerOption(opts ...grpc.ServerOption) {
	c.ServerOptions = opts
}
