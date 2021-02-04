package http

import (
	"net/http"
	"xonlab.com/frozra/v1/cache"
	"xonlab.com/frozra/v1/cluster"
)

type Server struct {
	cache.Cache
	cluster.Node
}

func (s *Server) Listen() {
	http.Handle("/cache/", s.cacheHandler())
	http.Handle("/status", s.statusHandler())
	http.Handle("/cluster", s.clusterHandler())
	http.Handle("/rebalance", s.rebalanceHandler())
	http.ListenAndServe(":9207", nil)
}

func New(c cache.Cache, n cluster.Node) *Server {
	return &Server{c, n}
}
