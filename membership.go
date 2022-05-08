package gossip

import (
	"github.com/treeforest/gossip/pb"
	"sync"
)

type membership struct {
	sync.RWMutex
	members map[string]*member
}

func newMembership() *membership {
	return &membership{members: map[string]*member{}}
}

func (s *membership) Load(id string) (*member, bool) {
	s.Lock()
	defer s.Unlock()
	m, ok := s.members[id]
	return m, ok
}

func (s *membership) Store(id string, m *member) {
	s.Lock()
	defer s.Unlock()
	s.members[id] = m
}

func (s *membership) Delete(id string) {
	s.Lock()
	defer s.Unlock()
	delete(s.members, id)
}

func (s *membership) Range(f func(m *member) bool) {
	s.RLock()
	defer s.RUnlock()
	for _, m := range s.members {
		if !f(m) {
			return
		}
	}
}

func (s *membership) Alive(n int) []*member {
	s.RLock()
	defer s.RUnlock()
	if n == -1 {
		n = len(s.members)
	}
	mbs := make([]*member, 0, n)
	for _, m := range s.members {
		if m.State() == pb.NodeStateType_Alive {
			mbs = append(mbs, m)
			n--
			if n == 0 {
				break
			}
		}
	}
	return mbs
}

func (s *membership) Rand() *member {
	s.RLock()
	defer s.RUnlock()
	for _, m := range s.members {
		return m
	}
	return nil
}

func (s *membership) Leave(localId string) {
	s.Lock()
	defer s.Unlock()
	for id, m := range s.members {
		_ = m.sendLeaveRequest(localId)
		m.close()
		delete(s.members, id)
	}
}
