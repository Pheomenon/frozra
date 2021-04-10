package tcp

import (
	"net"

	"github.com/Pheomenon/frozra/v1/cache"
	"github.com/Pheomenon/frozra/v1/cluster"
)

type Server struct {
	cache.Cache
	cluster.Node
}

func (s *Server) Listen() {
	l, e := net.Listen("tcp", s.Addr()+":9208")
	if e != nil {
		panic(e)
	}
	for {
		c, e := l.Accept()
		if e != nil {
			panic(e)
		}
		go s.process(c)
	}
}

func New(c cache.Cache, n cluster.Node) *Server {
	return &Server{c, n}
}
