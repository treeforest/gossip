package gossip

import (
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"os"
	"time"
)

type Config struct {
	// Id 节点id
	Id string

	// Name 节点名
	Name string

	// BindAddress 节点绑定的地址
	BindAddress string

	// BindPort 节点绑定的端口
	BindPort int32

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

	// ProbeInterval 探测节点状态的间隔
	ProbeInterval time.Duration

	// ProbeTimeout 探测节点状态的超时时间
	ProbeTimeout time.Duration

	// GossipInterval 广播消息的间隔时间
	GossipInterval time.Duration

	// GossipNodes 随机选取多少个节点进行发送,-1则表示发送给所有的邻居节点
	GossipNodes int

	// GossipToTheDeadTime
	GossipToTheDeadTime time.Duration

	// GossipMidCacheCap 对于广播消息id缓存的最大容量
	GossipMidCacheCap int

	// GossipMidCacheTimeout 广播消息id缓存的超时时间，超时后将会
	// 将消息id从缓存中删除
	GossipMidCacheTimeout time.Duration

	// PushPullInterval 状态同步间隔时间
	PushPullInterval time.Duration
}

func DefaultConfig() *Config {
	hostname, _ := os.Hostname()
	return &Config{
		Id:          uuid.New().String(),
		Name:        hostname,
		BindAddress: "0.0.0.0",
		BindPort:    4377, // 小游戏
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
		ServerOptions: []grpc.ServerOption{
			grpc.Creds(insecure.NewCredentials()),
		},
		Delegate:              nil,
		EventDelegate:         nil,
		ProbeInterval:         150 * time.Millisecond,
		ProbeTimeout:          2 * time.Second,
		GossipInterval:        100 * time.Millisecond,
		GossipNodes:           3,
		GossipToTheDeadTime:   1 * time.Second,
		GossipMidCacheCap:     1024 * 1024 * 4,
		GossipMidCacheTimeout: time.Second * 30,
		PushPullInterval:      200 * time.Millisecond,
	}
}

func (c *Config) SetDialOption(opts ...grpc.DialOption) {
	c.DialOptions = opts
}

func (c *Config) SetServerOption(opts ...grpc.ServerOption) {
	c.ServerOptions = opts
}
