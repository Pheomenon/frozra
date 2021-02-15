[![Build Status](https://travis-ci.org/Pheomenon/frozra.svg?branch=master)](https://travis-ci.org/Pheomenon/frozra)

# :star: Introduction

`frozra` is a distributed cache system. The data model is key-value. `frozra` was inspired by LevelDB, and also use LSM trees as the storage engine. To be precise, `frozra` use range LSM-tree that use hash string to store every key's position in disk.  Compared with the original LSM-tree, range LSM-tree's advantage is that it basically does not require any sorting for its elements when merge two different layer's table.

# 🚀 Features

- [x] Self-built cluster, no complicated configuration
- [x] Concise and easy-to-use APIs
- [x] Supporting asynchronous write operation
- [x] Use consistent hashing to achieve load balancing
- [x] Automatic node rebalancing, whether cluster stable or not, the number of elements stored in each node is approximately the same
- [ ] use built-in event-driven mechanisms: `epoll`

# :space_invader: License

Source code in `frozra` is available under the [MIT License](/LICENSE).

