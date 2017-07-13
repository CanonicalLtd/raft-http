raft-http [![Build Status](https://travis-ci.org/dqlite/raft-http.png)](https://travis-ci.org/dqlite/raft-http) [![Coverage Status](https://coveralls.io/repos/github/dqlite/raft-http/badge.svg?branch=master)](https://coveralls.io/github/dqlite/raft-http?branch=master) [![Go Report Card](https://goreportcard.com/badge/github.com/dqlite/raft-http)](https://goreportcard.com/report/github.com/dqlite/raft-http)  [![GoDoc](https://godoc.org/github.com/dqlite/raft-http?status.svg)](https://godoc.org/github.com/dqlite/raft-http)
=========

This repository provides the `rafthttp` package, which can be used to
establish a network connection between to raft nodes using HTTP. Once
the HTTP connection is established, the Upgrade header will be used to
switch it to raw TCP mode, and the regular TCP-based network transport
of the `raft` [package](https://github.com/hashicorp/raft) can take it
from there.

Documentation
==============

The documentation for this package can be found on [Godoc](http://godoc.org/github.com/dqlite/raft-http).
