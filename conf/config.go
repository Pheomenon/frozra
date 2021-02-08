package conf

import (
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type Persistence struct {
	L0Capacity      int    `yaml:"l0Capacity"`
	MemoryTableSize int    `yaml:"memoryTableSize"`
	L1TableSize     int    `yaml:"l1TableSize"`
	Path            string `yaml:"path"`
}

type Inmemory struct {
	MemoryThreshold uint64 `yaml:"memoryThreshold"`
}

type Conf struct {
	Inmemory
	Persistence
}

func LoadConfigure() Conf {
	C := Conf{}
	data, err := ioutil.ReadFile("../conf/conf.yml")
	if err != nil {
		logrus.Fatalf("open configure file error: %v", err)
	}
	err = yaml.Unmarshal(data, &C)
	if err != nil {
		logrus.Fatalf("parse configure file error: %v", err)
	}
	C.MemoryTableSize <<= 20
	C.L1TableSize <<= 20
	return C
}
