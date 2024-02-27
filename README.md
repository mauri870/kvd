# kvd

kvd is a distributed key-value store that uses the redis RESP protocol.
It achieves fault tolerance and high availability by using the Raft consensus algorithm.

The backing storage is pluggable, but currently it is backed by [etcd-io/bbolt](https://github.com/etcd-io/bbolt).

Only a subset of the RESP protocol is implemented, enough that you can toy with `redis-cli` and `redis-benchmark` with basic commands.

Here is a [visualization of how Raft works](https://thesecretlivesofdata.com/raft/).

## Usage

You can run a single node cluster with:

```bash
go build .
./kvd -addr localhost:6379 -raft-addr localhost:19000 ./node0
```

> Raft stores data in the `node0` directory.

The following subset of the Redis RESP protocol is implemented:

- `SET key value`
- `GET key`
- `DEL key`
- `PING`
- `QUIT`
- `CONFIG GET save`
- `CONFIG GET appendonly`

That means you can use `redis-cli` and `redis-benchmark` to some extent:

```bash
$ redis-cli PING
PONG
$ redis-cli SET a 42
OK
$ redis-cli GET a
"42"
$ redis-cli DEL a
OK

# partial support
$ redis-benchmark SET a 42
$ redis-benchmark GET a
$ redis-benchmark PING
```

## Performance

This is a toy project, so performance is not a really a concern.  As usual with Raft clusters, writes must be processed by the leader node and go trough the consensus mechanism so it is bottlenecked by network and disk/io.

With write operations it can achieve around ~500 ops/sec in a 3 node cluster. For reads, around ~160k ops/sec per node. Since reads scale with the cluster size, your read throughput can be multiplied by the number of nodes in the cluster.

## Forming a cluster

A goreman Procfile is included to start a 3-node cluster. You can start it with:

```bash
$ goreman start
```

With the cluster started, you must join the nodes to the cluster. You can do that by sending a `JOIN` command to the leader node:

```bash
$ redis-cli JOIN node1 localhost:19001
$ redis-cli JOIN node2 localhost:19002
```

All writes must be send to the leader, reads can be distributed between
nodes. For a three node cluster, the cluster can recover from one node
failure. If the leader node fails, the remaining nodes
will try to start an election as soon as 3 nodes are available. While a leader is elected, reads will still
continue to be processed.

```bash
# write to the leader
$ redis-cli SET a "hello"
OK

# read from any node
$ redis-cli -p 6380 GET a
```
