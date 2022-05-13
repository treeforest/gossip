package gossip

import (
	"github.com/treeforest/gossip/pb"
	"sync"
	"time"
)

type membershipManager struct {
	sync.RWMutex
	aliveExpirationTimeout time.Duration // alive 到 dead 的失效时间
	deadExpirationTimeout  time.Duration // dead 到移除的失效时间
	deadMembers            map[string]time.Time
	aliveMembers           map[string]time.Time
	id2Member              map[string]*pb.Membership
}

func newMembershipManager(aliveExpirationTimeout, deadExpirationTimeout time.Duration) *membershipManager {
	return &membershipManager{
		aliveExpirationTimeout: aliveExpirationTimeout,
		deadExpirationTimeout:  deadExpirationTimeout,
		deadMembers:            make(map[string]time.Time),
		aliveMembers:           make(map[string]time.Time),
		id2Member:              make(map[string]*pb.Membership),
	}
}

func (mgr *membershipManager) GetMembership() []pb.Membership {
	mgr.RLock()
	defer mgr.RUnlock()
	var members []pb.Membership
	for id, _ := range mgr.aliveMembers {
		members = append(members, *mgr.id2Member[id])
	}
	return members
}

// GetAliveMembers 获取除了 i d为 expect 的 num 个活着的成员
func (mgr *membershipManager) GetAliveMembers(num int, expect string) []pb.Membership {
	mgr.RLock()
	defer mgr.RUnlock()
	n := len(mgr.aliveMembers)
	k := num
	if k > n {
		k = n
	}
	var members []pb.Membership
	i := 0
	for id, _ := range mgr.aliveMembers {
		if i >= k {
			break
		}
		if id == expect {
			continue
		}
		i++
		members = append(members, *mgr.id2Member[id])
	}
	return members
}

// ExpireDeadMembers 到期的成员，将其从 alive -> dead
func (mgr *membershipManager) ExpireDeadMembers(dead []string) []pb.Membership {
	var deadMembers []pb.Membership

	mgr.Lock()
	for _, id := range dead {
		lastTime, isAlive := mgr.aliveMembers[id]
		if !isAlive {
			continue
		}
		deadMembers = append(deadMembers, *mgr.id2Member[id])
		mgr.deadMembers[id] = lastTime
		delete(mgr.aliveMembers, id)
	}
	mgr.Unlock()

	return deadMembers
}

func (mgr *membershipManager) CheckAliveMembers() []string {
	mgr.RLock()
	defer mgr.RUnlock()
	var dead []string
	for id, last := range mgr.aliveMembers {
		elapsedNonAliveTime := time.Since(last) // 失效时间
		if elapsedNonAliveTime.Nanoseconds() > mgr.aliveExpirationTimeout.Nanoseconds() {
			dead = append(dead, id)
		}
	}
	return dead
}

// CheckExpirationMember 检查失效的成员，然后从记录中删除
func (mgr *membershipManager) CheckExpirationMember() []pb.Membership {
	var expirations []string
	mgr.RLock()
	for id, last := range mgr.deadMembers {
		elapsedNonAliveTime := time.Since(last) // 失效时间
		if elapsedNonAliveTime.Nanoseconds() > mgr.deadExpirationTimeout.Nanoseconds() {
			expirations = append(expirations, id)
		}
	}
	if len(expirations) == 0 {
		mgr.RUnlock()
		return []pb.Membership{}
	}
	mgr.RUnlock()

	var members []pb.Membership

	mgr.Lock()
	for _, id := range expirations {
		// 再次检查，可能在这期间已经被删除了
		m, ok := mgr.id2Member[id]
		if !ok {
			continue
		}
		members = append(members, *m)
		delete(mgr.deadMembers, id)
		delete(mgr.id2Member, id)
	}
	mgr.Unlock()

	return members
}

func (mgr *membershipManager) DeadToSlice() []pb.Membership {
	mgr.RLock()
	defer mgr.RUnlock()
	var dead []pb.Membership
	for id, _ := range mgr.deadMembers {
		dead = append(dead, *mgr.id2Member[id])
	}
	return dead
}

func (mgr *membershipManager) AliveToSlice() []pb.Membership {
	mgr.RLock()
	defer mgr.RUnlock()
	var alive []pb.Membership
	for id, _ := range mgr.aliveMembers {
		alive = append(alive, *mgr.id2Member[id])
	}
	return alive
}

func (mgr *membershipManager) IsAlive(id string) bool {
	mgr.RLock()
	defer mgr.RUnlock()
	_, isAlive := mgr.aliveMembers[id]
	return isAlive
}

func (mgr *membershipManager) Alive(id string) {
	mgr.Lock()
	defer mgr.Unlock()
	_, ok := mgr.aliveMembers[id]
	if !ok {
		return
	}
	mgr.aliveMembers[id] = time.Now()
}

func (mgr *membershipManager) Load(id string) (*pb.Membership, bool) {
	mgr.RLock()
	defer mgr.RUnlock()
	m, ok := mgr.id2Member[id]
	return m, ok
}

func (mgr *membershipManager) LoadAndDelete(id string) (*pb.Membership, bool) {
	mgr.Lock()
	defer mgr.Unlock()
	m, ok := mgr.id2Member[id]
	if !ok {
		return nil, false
	}
	delete(mgr.deadMembers, id)
	delete(mgr.aliveMembers, id)
	delete(mgr.deadMembers, id)
	return m, true
}

func (mgr *membershipManager) Delete(id string) {
	mgr.LoadAndDelete(id)
}

// LearnNewMembers 学习新成员
func (mgr *membershipManager) LearnNewMembers(aliveMembers []pb.Membership, deadMembers []pb.Membership) {
	mgr.Lock()
	defer mgr.Unlock()
	for _, member := range aliveMembers {
		mgr.aliveMembers[member.Id] = time.Now()
		mgr.id2Member[member.Id] = &member
	}
	for _, member := range deadMembers {
		mgr.deadMembers[member.Id] = time.Now()
		mgr.id2Member[member.Id] = &member
	}
}

// ResurrectMember 复活成员或更新成员
func (mgr *membershipManager) ResurrectMember(member *pb.Membership) {
	mgr.Lock()
	defer mgr.Unlock()
	id := member.Id
	mgr.aliveMembers[id] = time.Now()
	mgr.id2Member[id] = member
	delete(mgr.deadMembers, id)
}
