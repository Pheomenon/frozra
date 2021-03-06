package main

import (
	"flag"
	"log"

	"github.com/Pheomenon/frozra/v1/cache"
	"github.com/Pheomenon/frozra/v1/cluster"
	"github.com/Pheomenon/frozra/v1/http"
	"github.com/Pheomenon/frozra/v1/tcp"
)

func main() {
	ttl := flag.Int("ttl", 30, "cache time to live")
	node := flag.String("node", "127.0.0.1", "node address")
	clus := flag.String("cluster", "", "cluster address")
	flag.Parse()
	log.Println("ttl is", *ttl)
	log.Println("node is", *node)
	log.Println("cluster is", *clus)
	c := cache.New(*ttl)
	n, e := cluster.New(*node, *clus)
	if e != nil {
		panic(e)
	}
	go tcp.New(c, n).Listen()
	http.New(c, n).Listen()
}
