package gossip

type Delegate interface {
	// NotifyMsg 当收到用户数据时，将会回调该接口，将用户数据传递
	// 给上层进行数据处理
	// 参数：
	// 		data: 从网络中接收到的消息
	NotifyMsg(data []byte)

	// GetBroadcasts 获取广播的消息。系统将定时调用该接口，将从该
	// 接口获取的消息广播到到整个网络（注：并不会广播给自身节点）
	// 返回值：
	// 		需要广播的消息
	GetBroadcasts() [][]byte

	// LocalState 获取本地状态信息
	// 参数：
	// 		join: 自身节点是否是刚加入网络，若是则为true;否则为false
	// 返回值：
	// 		本地节点的状态信息
	LocalState(join bool) []byte

	// MergeRemoteState 合并远端节点的状态
	// 参数：
	// 		buf: 远端节点的状态信息
	//		join: 自身节点是否是刚加入网络，若是则为true;否则为false
	MergeRemoteState(buf []byte, join bool)
}
