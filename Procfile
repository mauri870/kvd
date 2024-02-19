# Use goreman to run this cluster `go install github.com/mattn/goreman@latest`
#   goreman start
#
# This is an example of how to run a 3 node cluster on a single machine.
node0: go run . -addr localhost:6379 -raft-addr localhost:19000 ./node0
node1: go run . -addr localhost:6380 -raft-addr localhost:19001 ./node1
node2: go run . -addr localhost:6381 -raft-addr localhost:19002 ./node2