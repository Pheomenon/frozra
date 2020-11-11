package main

import (
	"flag"
	"log"
	"xonlab.com/frozra/v1/cache"
	"xonlab.com/frozra/v1/cluster"
	"xonlab.com/frozra/v1/http"
	"xonlab.com/frozra/v1/tcp"
)

func main() {
	typ := flag.String("type", "inmemory", "cache type")
	node := flag.String("node", "127.0.0.1", "node address")
	clus := flag.String("cluster", "", "cluster address")
	flag.Parse()
	log.Println("type is ", *typ)
	log.Println("node is ", *node)
	log.Println("cluster is ", *clus)
	c := cache.New(*typ)
	n, e := cluster.New(*node, *clus)
	if e != nil {
		panic(e)
	}
	go tcp.New(c, n).Listen()
	http.New(c, n).Listen()
}
