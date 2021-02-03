package persistence

import (
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type Conf struct {
	L0Capacity      int    `yaml:"l0Capacity"`
	MemoryTableSize int    `yaml:"memoryTableSize"`
	Path            string `yaml:"Path"`
	L1TableSize     int    `yaml:"l1TableSize"`
}

func LoadConfigure() Conf {
	c := Conf{}
	data, err := ioutil.ReadFile("./conf.yml")
	if err != nil {
		logrus.Fatalf("open configure file error: %v", err)
	}
	err = yaml.Unmarshal(data, &c)
	if err != nil {
		logrus.Fatalf("parse configure file error: %v", err)
	}
	c.MemoryTableSize <<= 20
	c.L1TableSize <<= 20
	return c
	//return Conf{
	//	L0Capacity:      3,
	//	MemoryTableSize: 64 << 20, // 64MB
	//	Path:            "./",
	//	L1TableSize:     64 << 21,
	//}
}
