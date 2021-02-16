[![Build Status](https://travis-ci.org/Pheomenon/frozra.svg?branch=master)](https://travis-ci.org/Pheomenon/frozra)
[![codecov](https://codecov.io/gh/Pheomenon/frozra/branch/master/graph/badge.svg?token=6PAVA00XGZ)](https://codecov.io/gh/Pheomenon/frozra)
![](https://img.shields.io/github/license/Pheomenon/frozra?color=blue)
![](https://img.shields.io/github/go-mod/go-version/Pheomenon/frozra)
![](https://img.shields.io/github/last-commit/Pheomenon/frozra?color=purple)
![](https://img.shields.io/github/stars/Pheomenon?affiliations=OWNER&style=social)
# :star: Introduction

`frozra` is a distributed cache system. The data model is key-value. `frozra` was inspired by LevelDB, and also use LSM trees as the storage engine. To be precise, `frozra` use range LSM-tree that use hash string to store every key's position in disk.  Compared with the original LSM-tree, range LSM-tree's advantage is that it basically does not require any sorting for its elements when merge two different layer's table.

# 🚀 Features

- [x] Self-built cluster, no complicated configuration
- [x] Concise and easy-to-use APIs
- [x] Supporting asynchronous write operation
- [x] Use consistent hashing to achieve load balancing
- [x] Automatic node rebalancing, whether cluster stable or not, the number of elements stored in each node is approximately the same
- [ ] use built-in event-driven mechanisms: `epoll`

# :zap: Performance
```
# Hardware Environment
CPU: 8 Virtual CPUs
Mem: 32GiB RAM
OS : Ubuntu-20.04.1 5.8.0-43-generic #49-Ubuntu
Go : go1.15.x linux/amd64
```
This benchmark script is in `benchmark` directory
![get](https://raw.githubusercontent.com/Pheomenon/frozra/master/readme_source/get.png)
![set](https://raw.githubusercontent.com/Pheomenon/frozra/master/readme_source/set.png)

# :space_invader: License

Source code in `frozra` is available under the [MIT License](/LICENSE).
