#!/bin/bash
set -ex

logfile=D:\WorkSpace\Master\Lab3\raft\part1\log.txt

go test -v -race -run $@ |& tee ${logfile}

go run ../tools/raft-testlog-viz/main.go < ${logfile}
