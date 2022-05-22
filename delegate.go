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

	// GetPullRequest 返回 pull 请求时所携带的信息。
	// 返回值：
	//     req: pull请求信息
	GetPullRequest() (req []byte)

	// ProcessPullRequest 处理pull请求，并返回结果。
	// 参数：
	//     req: 远程节点的 pull 请求信息
	// 返回值：
	//     resp: pull 响应消息
	ProcessPullRequest(req []byte) (resp []byte)

	// ProcessPullResponse 合并远程节点返回的状态信息。
	// 参数：
	//     resp: 远程节点返回的 pull 响应消息
	ProcessPullResponse(resp []byte)
}
