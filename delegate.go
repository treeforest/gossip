package gossip

type Delegate interface {
	// NotifyMsg 处理用户数据
	// 参数：
	// 		msg: 用户数据
	NotifyMsg(msg []byte)

	// GetBroadcasts 返回需要进行gossip广播的数据。
	// 返回值：
	// 	   data: 待广播的消息
	GetBroadcasts() (data [][]byte)

	// Summary 返回 pull 请求时所携带的信息。
	// 返回值：
	//     req: pull请求信息
	Summary() (req []byte)

	// LocalState 返回相关的本地状态信息。
	// 参数：
	//     summary: 远程节点的 pull 请求信息
	// 返回值：
	//     state: 状态数据
	LocalState(summary []byte) (state []byte)

	// MergeRemoteState 合并远程节点返回的状态信息。
	// 参数：
	//     state: 远程节点返回的状态信息
	MergeRemoteState(state []byte)
}
