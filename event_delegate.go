package gossip

import "github.com/treeforest/gossip/pb"

type EventDelegate interface {
	// NotifyJoin 通知节点加入网络
	NotifyJoin(*pb.Node)

	// NotifyLeave 通知节点离开网络
	NotifyLeave(*pb.Node)

	// NotifyUpdate 当节点的元数据发生改变时，通知节点更新
	NotifyUpdate(*pb.Node)
}
