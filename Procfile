# Use goreman to run this cluster:
#   go tool goreman start
#
# This is an example of how to run a 3 node cluster on a single machine.
node0: go run . -addr localhost:6379 -raft-addr localhost:19000 -http-addr localhost:8080 ./node0
node1: go run . -addr localhost:6380 -raft-addr localhost:19001 -http-addr localhost:8081 ./node1
node2: go run . -addr localhost:6381 -raft-addr localhost:19002 -http-addr localhost:8082 ./node2