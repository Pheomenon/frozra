package cache

import (
	"github.com/sirupsen/logrus"
)

func New(ttl int) Cache {
	var c Cache
	c = newInMemoryCache(ttl)
	logrus.Info("frozra is ready to serve!")
	return c
}
