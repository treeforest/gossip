# gossip

Implementation of gossip based on pull mechanism.

## Usage
For usage details, please refer to the example folder.

User must implement interface **Delegate**
* Delegate
```go
type Delegate interface {
	// NotifyMsg notify user massage
	NotifyMsg(msg []byte)

	// GetBroadcasts gossip user messages
	GetBroadcasts() (data [][]byte)

	// GetPullRequest return pull request
	GetPullRequest() (req []byte)

	// ProcessPullRequest process pull request and return pull response
	ProcessPullRequest(req []byte) (resp []byte)

	// ProcessPullResponse process pull response
	ProcessPullResponse(resp []byte)
}
```

User selectable interface **EventDelegate**
* EventDelegate
```go
type EventDelegate interface {
	// NotifyJoin notify membership join
	NotifyJoin(member pb.Membership)

	// NotifyLeave notify membership leave
	NotifyLeave(member pb.Membership)

	// NotifyUpdate notify membership metadata update
	NotifyUpdate(member pb.Membership)
}
```

## Pull Model
![pull.png](pull.png)