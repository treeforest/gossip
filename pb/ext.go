package pb

import "fmt"

func (m *Node) FullAddress() string {
	return fmt.Sprintf("%s:%d", m.Ip, m.Port)
}
