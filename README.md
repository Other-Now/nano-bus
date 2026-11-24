# nano-bus

[![ci](https://github.com/Other-Now/nano-bus/actions/workflows/ci.yml/badge.svg)](https://github.com/Other-Now/nano-bus/actions/workflows/ci.yml)

A small low-latency publish/subscribe middleware in Java 21: topic sharding, an append-only journal, and a
replicated standby that takes over without losing or repeating a message. The broker and the clients use only
the JDK (about 2,100 lines); HdrHistogram is used by the benchmark.

```
publishers ──TCP──▶ ┌── leader ───────────────┐   REPL_DATA   ┌── standby ──┐
                    │ shard 0: one thread,    │ ────────────▶ │ shard 0     │
                    │   one port, one journal │ ◀──────────── │ journal     │
                    │ shard 1: ...            │   REPL_ACK    │ ...         │
subscribers ◀─TCP── └───────────┬─────────────┘               └──────┬──────┘
                                └────── lease, epoch, in-sync set ───┘
                                             witness
```

## Design

- **Shared-nothing shards.** One shard is one thread, one port and one journal. Topics map to shards by hash.
  There are no locks on the message path.
- **Event-loop turn:** read sockets → append → send to the standby → one `write()` for the batch → advance the
  commit index → ack publishers and fan out. Load makes the batches bigger by itself.
- **The journal record is the wire record.** Fan-out and replication copy bytes; nothing is re-encoded.
- **Consumers read from the log.** Caught-up readers copy from the active segment in memory, slow ones read from
  disk. A slow consumer costs the broker no extra memory. If retention passes it, it gets an explicit GAP message.
- **Nothing is acked or delivered before the standby has it** (while a standby is in sync).
- **Producers are idempotent.** Each message carries a producer id and a dense sequence. The broker rebuilds its
  table of last sequences from the journal, so retries are dropped even after a failover.

### Failover and split-brain

A third process, the witness, stores `(epoch, leader, in-sync set, lease)`. It never sees a message.

1. The leader adds the standby to the set only when the standby holds everything committed. It must remove it
   before committing anything without it. So every node in the set has every committed message.
2. A takeover needs an expired lease **and** membership in the set. It bumps the epoch.
3. The standby stops acking the old leader *before* it asks for the lease, so a paused leader that wakes up
   cannot commit anything.
4. A rejoining node cuts its log where it stops agreeing with the leader's (Kafka's leader-epoch rule).

The lease only decides *when* a takeover may happen. Safety does not depend on clocks, and traffic continues
while the witness is down.

## Results

All numbers are from GitHub Actions (4 vCPU, Linux, loopback TCP, leader + standby + load on one machine),
[run 45393567738](https://github.com/Other-Now/nano-bus/actions/runs/45393567738). Raw files: [results/ci](results/ci).
Each job may get a different machine, so compare rows within a table, not across tables.

### Chaos: kill -9 and pauses under 20k messages/s

Two independent subscribers are compared with what publishers had acked. After the run, both journals are compared
record by record.

| Mode | Messages acked | kill -9 | Leader pauses (2 s) | Lost | Duplicated |
|---|---|---|---|---|---|
| **safe** (300 s) | 6,081,635 | 94 (leader, standby, witness) | 16 | **0** | **0** |
| naive (90 s): ack before replication, promote whoever is up | 1,867,188 | 29 | 5 | **150,902** | 0 |
| no producer sequences (90 s) | 1,869,975 | 29 | 5 | 0 | **73** |

- Ack stall when the leader is killed: median 426 ms, max 683 ms (31 kills). The lease is 400 ms.
- Ack stall when the leader is paused with SIGSTOP: 512–517 ms. Standby killed: 412–454 ms.
- If the leader dies while the standby is still catching up, the cluster **waits** for the old leader. The naive
  control does not wait, and that is where its losses come from.

### Latency: publish → journal → standby → commit → subscriber

Open loop, 128-byte payload, one shard, latency measured from the time each message was *due*. Three runs each.

| Rate/s | p50 | p99 | p99.9 |
|---|---|---|---|
| 50,000 | 127–128 µs | 0.21–0.27 ms | 1.0–1.7 ms |
| 100,000 | 132–133 µs | 0.22–0.33 ms | 0.9–2.6 ms |
| 200,000 | 125–126 µs | 1.5–1.9 ms | 4.1–4.3 ms |
| 300,000 | 128–131 µs | 1.9–2.1 ms | 4.2–4.5 ms |
| 500,000 | 137–140 µs | 2.5–2.8 ms | 4.4–4.7 ms |

- The broker allocates about 0.1 byte per message and ran **0 GCs** in the measured window.
- Without a standby: 66–99 µs p50, 0.25–0.35 ms p99 at 500k/s.
- The runner itself is not quiet: four threads that only read the clock see p99.9 near 3 ms.
- `--fsync` (force every batch) costs: p50 0.45 ms at 20k/s, and it saturates below 100k/s. On another CI machine it
  was three times slower at 20k/s, so treat it as disk-dependent.

### Finding 1: segment housekeeping was the tail

In the first version p99 grew with the rate, up to 8.4 ms at 500k/s. Rolling a 16 MB segment took 0.5–1.9 ms for the
snapshot and the new file plus 2.3–5 ms to `unlink` the oldest one (timed on a laptop under WSL2), on the event loop,
on both nodes. Snapshots and deletions moved to a housekeeping thread. Same CI machine, interleaved, 4 runs each:

| | 100k/s p99.9 | 300k/s p99 |
|---|---|---|
| on the event loop | 5.0–5.2 ms | 3.8–4.0 ms |
| off the event loop | 1.0–1.7 ms | 1.5–1.6 ms |

### Finding 2: which collector? It depends on whether you allocate

At 100k/s, 1 GB heap. "Heap cache" keeps the last 1M messages as objects (about 230 MB live).

| | Off-heap design: GCs, p99.9 | Heap cache: p99.9 | Heap cache: longest pause |
|---|---|---|---|
| G1 | 0, 2.0–2.2 ms | 14–17 ms | 22–26 ms |
| Parallel | 0, 1.9–2.4 ms | 89–103 ms | 116–129 ms |
| Shenandoah | 0, 2.2–2.3 ms | 1.4–2.1 ms | 0.14 ms |
| ZGC | 0, 1.9–2.1 ms | 1.3–1.6 ms | 0.03 ms |
| Generational ZGC | 0, 2.2–2.5 ms | 1.5–1.9 ms | 0.03 ms |

p50 does not move with the collector (176–181 µs off-heap, 159–164 µs with the cache, on two different machines). One
caution: on an earlier, slower runner non-generational ZGC had p99.9 of 10–40 ms with the same sub-millisecond
pauses, so "short pauses" alone is not a latency result.

### Finding 3: a leak that does not show in the heap

`--plant-leak` adds a bug on purpose: closed subscribers stay in a registry. Each is a small object that holds two
256 KB direct buffers.

- The broker died after 480 connections with a *direct-memory* OutOfMemoryError. Its heap was below 13 MB of 64 MB.
- [scripts/leak-hunt.sh](scripts/leak-hunt.sh) finds it with JDK tools only: metrics (heap flat, direct memory
  rising, 0 subscribers) → native memory tracking (`Other`) → two class histograms (`Conn` 5.6 times as many) → JFR
  old-object sample, whose reference chain ends in `Shard.sessionsById`. Output:
  [results/ci/leak/REPORT.txt](results/ci/leak/REPORT.txt).
- Without any leak, allocating direct buffers per connection still swung up to 232 MB, because a broker that hardly
  allocates hardly ever GCs, and only a GC frees direct buffers. Pooling them keeps it flat at 17 MB.

### Alerting

Prometheus rules in [monitoring/alerts.yml](monitoring/alerts.yml), unit-tested with `promtool` and end to end in CI:
healthy cluster under load → no alert; topic stops publishing → 14.9 s; subscriber stops reading → 17.1 s;
standby killed → 16.4 s; both killed → 5.9 s.

## Tests

23 unit and cluster tests (journal model test, torn tails, divergent logs, stale standby, malformed frames).
[scripts/mutants.py](scripts/mutants.py) breaks 16 safety rules one at a time: 16 of 16 are caught
([results/mutants.txt](results/mutants.txt)). The first run caught 14; two tests were added for the other two.

## Run it

```bash
mvn package                                     # JDK 21
java -jar target/nano-bus.jar                   # lists the commands
java -jar target/nano-bus.jar chaos --mode safe --seconds 60 --out results/local     # Linux: includes SIGSTOP faults
java -jar target/nano-bus.jar bench --rates 50000,100000 --out results/local
```

## Limits

- One machine, loopback TCP. No UDP or multicast, no TLS, no authentication, no consumer groups.
- Two copies, not a quorum. The witness is one process: without it traffic continues, but there is no failover.
- Crash-safe against process death. Without `--fsync`, losing power on both nodes at once loses recent messages, and
  wiping an in-sync standby's disk breaks the in-sync promise until it has caught up again.
- More than one shard works and is tested, but the latency table is one shard: a 4-vCPU runner cannot show scaling.
- Process kills and pauses were tested; network partitions were not, beyond cutting the witness connections.
