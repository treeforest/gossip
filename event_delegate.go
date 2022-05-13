package gossip

import "github.com/treeforest/gossip/pb"

type EventDelegate interface {
	// NotifyJoin 新成员加入集群的事件通知
	// 参数：
	//     member: 新加入集群的成员信息
	NotifyJoin(member pb.Membership)

	// NotifyLeave 成员离开集群的事件通知
	// 参数：
	//     member: 离开集群的成员信息
	NotifyLeave(member pb.Membership)

	// NotifyUpdate 成员的元数据更新的事件通知
	// 参数：
	//     member: 元数据更新的成员信息
	NotifyUpdate(member pb.Membership)
}
