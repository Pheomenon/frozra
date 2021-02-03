package persistence

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"testing"
)

func TestConf(t *testing.T) {
	type Conf struct {
		L0Capacity     int    `yaml:"l0Capacity"`
		MemTableSize   string `yaml:"MemoryTableSize"`
		Path           string `yaml:"Path"`
		MaxL1TableSize string `yaml:"maxL1TableSize"`
	}
	c := Conf{}
	data, _ := ioutil.ReadFile("./conf.yml")
	err := yaml.Unmarshal(data, &c)
	if err != nil {
		panic(err)
	}
	fmt.Printf("--- t:\n%#v\n\n", c)
}
