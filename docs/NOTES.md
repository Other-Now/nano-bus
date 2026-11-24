# Study notes

Read in this order; about a day for the code, half a day for the stories.

| # | File | What to be able to explain |
|---|---|---|
| 1 | `Proto.java` | frame layout, record layout, why the record is also the wire format |
| 2 | `Journal.java` | append, flush, roll, read with a cursor, recovery, truncation, `commonPrefix` |
| 3 | `Shard.java` | `run`, `leaderTurn`, `onPublish`, `pump`, `trackReplica`, `standbyTurn` |
| 4 | `Witness.java`, `Broker.java` | lease, epoch, in-sync set, `standbyTick`, `leaderTick` |
| 5 | `Publisher.java`, `Subscriber.java`, `Link.java` | window, resend, resume |
| 6 | `harness/Chaos.java`, `harness/Bench.java` | what is checked, how latency is measured |

## One message, end to end

1. `Publisher.offer` writes a PUBLISH frame into its window (8 MB direct buffer) with the next producer sequence.
2. The leader's shard thread reads the socket. `onPublish` checks the sequence and `Journal.append` copies the topic
   and payload from the socket buffer into the segment buffer and adds the CRC32C.
3. `leaderTurn`: `pump(replica)` copies the new records to the standby's socket, then `journal.flush()` does one `write()`.
4. The standby appends the same bytes, checks the CRC, writes, and sends REPL_ACK(lastSeq).
5. The leader sets `commit = min(flushed, replica.ackSeq)`. It sends PUB_ACK and pumps subscribers up to `commit`.
6. `Subscriber` drops anything below the sequence it wants, so a reconnect has no repeats.

## The safety argument (learn this one by heart)

Claim: an acked message is never lost, and a delivered message is never replaced.

- Acked or delivered means `seq <= commit`.
- While the standby is in the in-sync set, commit needs its ack. The leader can drop that requirement only *after* the
  witness has removed the standby from the set.
- So at any time every node in the set holds every committed record.
- Only a node in the set can take over. It adopts its whole log. Nothing committed is missing.
- Old leader still alive? The standby stopped acking before it asked for the lease, and the witness rejects the old
  leader's REMOVE because the epoch moved. So the old leader can never commit again.
- Old leader rejoins: its extra records were never committed, so cutting them is safe.

Why not only a lease? A leader can check its lease, then be paused for 2 s (GC, SIGSTOP), then act. Here that does no
harm because the commit needs the standby. The lease affects only availability.

**The race I found in review:** the leader sends ADD(standby), the call times out, the leader sends REMOVE and goes on
alone, then the delayed ADD arrives. The stale standby would be in the set. Fix: every change carries the version it
saw; the witness rejects an old one (`WitnessTest.aLateAddCannotUndoARemove`).

**Compared with Kafka:** the set is the ISR, the witness is the controller, `commonPrefix` is KIP-101 leader epochs,
producer sequences are the idempotent producer (KIP-98), and `pump` from the log is how Kafka serves consumers.
**Compared with Raft:** two copies instead of three, the witness decides elections, no log-matching per append.

## The five stories

1. **The tail that was `unlink()`.** p99 grew with the rate, and p99.9 sat near 9-11 ms. The
   fraction of slow messages matched (segment rolls per second) x (a roll on each of two nodes). Timed the roll: 2.3–5 ms to unlink plus
   0.5–1.9 ms for snapshot and create, on both nodes. Moved off the event loop, keeping the order "successor's
   snapshot first, then delete", because recovery starts from the oldest snapshot. Proved it by an interleaved A/B on one machine.
2. **My harness was the tail.** Before that, p99.9 was about 100 ms in every run. Max was about 100 ms, so I suspected the
   100 ms heartbeat and changed it to 25 ms: no effect. One 100 ms stall per run gives exactly p99 ≈ 27 ms. It was the cold
   HTTP metrics scrape on the publishing thread. After: 4.5 ms.
3. **A control that could not fail.** With producer dedup switched off, the first chaos runs still had 0 duplicates. The
   reconnect handshake tells the publisher what is committed, so it had nothing to resend. Only with that off too
   did duplicates appear (73). A control that never fails proves nothing, so the harness exits non-zero if it does not.
4. **The off-heap leak.** Heap under 13 of 64 MB and yet OutOfMemoryError. See README, finding 3. The related real problem: a
   zero-allocation broker never runs GC, and only GC frees direct buffers, so they had to be pooled.
5. **A short frame could kill the broker.** The malformed-frame test showed that a frame shorter than its type needs
   read past its end; the exception ended the shard thread and the process. Fixed with a minimum length per type.
   (Also a harness bug: the drain loop could exit at once because its deadline was still 0.)

## JVM questions this project prepares

- **Why direct buffers?** Heap buffers are copied into a temporary direct buffer by NIO on each call, and live data in
  the heap has to be marked and moved. Cost: freed only when the owner is collected (Cleaner), limited by `MaxDirectMemorySize`.
- **What happens at that limit?** `Bits.reserveMemory` calls `System.gc()`, retries, then throws OutOfMemoryError. With
  `-XX:+DisableExplicitGC` the GC does not happen, so this is a common production trap.
- **G1 vs ZGC vs Shenandoah.** G1 evacuates while stopped; the pause grows with live young data, here 22–26 ms. ZGC
  (colored pointers, load barriers) and Shenandoah (load-reference barriers) move objects concurrently, so pauses stay under
  1 ms but use CPU and need headroom; if they fall behind, the thread stalls on allocation (`Allocation Stall`).
- **Why is G1 bad here?** A 1M-message ring keeps every new object alive until it is overwritten, so the
  generational hypothesis fails: each young GC copies nearly everything.
- **How was 0.1 B/message measured?** `ThreadMXBean.getThreadAllocatedBytes` on the shard thread, divided by messages.
- **What does allocate?** Lambdas and iterators were avoided in the loop: `selector.select(Consumer)` with one stored
  consumer, indexed loops over `CopyOnWriteArrayList`, topics looked up by bytes.
- **Finding a leak in production:** metrics trend → NMT (which category) → `GC.class_histogram` twice (which class) → JFR
  `OldObjectSample` with `path-to-gc-roots` (who holds it). A heap dump answers the same question but pauses the process.
- **Coordinated omission:** latency is taken from the time a message was due. If the broker stalls 100 ms at 100k/s,
  10,000 messages are late, not one.

## Likely hard questions

- **"0 lost over 94 kills, is that a proof?"** No. It is evidence, plus the argument above, plus 16 mutants that the tests
  catch. Not covered: real network partitions, power loss, disk corruption on the only in-sync copy.
- **"Why does killing the standby stall acks for 440 ms?"** The leader waits `replTimeoutMs` (400) before it removes it.
  Shorter means more false removals when the standby has a pause.
- **"What if the witness dies, then the leader?"** No failover until the witness is back. In production it would be etcd or ZooKeeper.
- **"Why not write to the disk with fsync?"** Two machines' page caches survive any single process or OS crash.
  `--fsync` exists; it cost 0.45 ms p50 at 20k/s and saturated below 100k/s on the CI disk.
- **"What would you add first?"** Multicast fan-out, consumer groups, three copies, and a partition test with `tc netem`/iptables.
- **"Who wrote the leak?"** I planted it to practise the method. Say so. Stories 1, 2, 3 and 5 were real.

## Numbers to know

p50 132 µs and p99 0.22–0.33 ms at 100k/s; p50 138 µs, p99 2.5–2.8 ms at 500k/s; 6.08M messages, 94 kills, 16 pauses, 0/0;
controls 150,902 lost and 73 duplicated; failover median 426 ms; G1 22–26 ms pause vs 0.03 ms ZGC; p99.9 5.1 → about 1.4 ms.
Always add: CI runner, loopback, one machine.
