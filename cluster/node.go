package cluster

import (
	"io/ioutil"
	"time"

	"github.com/hashicorp/memberlist"
	"stathat.com/c/consistent"
)

type Node interface {
	ShouldProcess(key string) (string, bool)
	Members() []string //offer cluster's members list
	Addr() string      //access node address
}

type node struct {
	*consistent.Consistent
	addr string
}

func (n *node) Addr() string {
	return n.addr
}

// ShouldProcess return which node should process this key
func (n *node) ShouldProcess(key string) (string, bool) {
	addr, _ := n.Get(key)
	return addr, addr == n.addr
}

func New(addr, cluster string) (Node, error) {
	conf := memberlist.DefaultLANConfig()
	conf.Name = addr
	conf.BindAddr = addr
	conf.LogOutput = ioutil.Discard
	l, e := memberlist.Create(conf)
	if e != nil {
		return nil, e
	}
	if cluster == "" {
		cluster = addr
	}
	clu := []string{cluster}
	_, e = l.Join(clu)
	if e != nil {
		return nil, e
	}
	circle := consistent.New()
	circle.NumberOfReplicas = 256
	go func() {
		for {
			m := l.Members()
			nodes := make([]string, len(m))
			for i, n := range m {
				nodes[i] = n.Name
			}
			circle.Set(nodes)
			time.Sleep(time.Second)
		}
	}()
	return &node{circle, addr}, nil
}
