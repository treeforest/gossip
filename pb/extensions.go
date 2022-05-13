package pb

func (m *Membership) Peer() *RemotePeer {
	return &RemotePeer{Id: m.Id, Endpoint: m.Endpoint}
}
