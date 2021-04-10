package conf

import (
	"fmt"
	"io/ioutil"
	"testing"

	"gopkg.in/yaml.v2"
)

func TestConf(t *testing.T) {
	type Conf struct {
		L0Capacity      int    `yaml:"l0Capacity"`
		MemoryTableSize int    `yaml:"memoryTableSize"`
		L1TableSize     int    `yaml:"l1TableSize"`
		Path            string `yaml:"path"`
	}
	c := Conf{}
	data, _ := ioutil.ReadFile("./conf.yml")
	err := yaml.Unmarshal(data, &c)
	if err != nil {
		panic(err)
	}
	fmt.Printf("--- t:\n%v\n\n", c)
}

func TestLoadConfigure(t *testing.T) {
	conf := LoadConfigure()
	fmt.Printf("--- t:\n%v\n\n", conf)
}
