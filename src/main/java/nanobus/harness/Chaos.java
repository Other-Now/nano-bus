package nanobus.harness;

import nanobus.Args;
import nanobus.JournalDiff;
import nanobus.Publisher;
import nanobus.Subscriber;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.locks.LockSupport;

/**
 * Publishes at a fixed rate while processes are killed and paused, then checks what two independent
 * subscribers saw against what was acked.
 *
 *   safe     ack after the standby has it, eligible set, producer sequences  -> must be 0 lost, 0 duplicated
 *   naive    ack after the local write, promote whichever node is up          -> control, loses messages
 *   nodedup  safe, but the broker ignores producer sequences                  -> control, duplicates messages
 */
public final class Chaos {
    private static final int PRODUCERS = 2;

    private final int shards, rate;
    private final Cluster cluster;
    private volatile boolean stopPublishing, stopAll;
    private volatile long lastAckProgressNs = System.nanoTime(), longestStallNs;
    private final long[][] acked;
    private volatile long resent, offeredTotal, ackedTotal;

    /** What one subscriber saw. */
    private final class Seen implements Subscriber.Handler {
        final BitSet[][] got = new BitSet[shards][PRODUCERS];
        final long[][] lastPseq = new long[shards][PRODUCERS];
        final long[] digest = new long[shards];
        long messages, dups, reordered, gaps, corrupt;
        volatile long progressNs = System.nanoTime();

        Seen() { for (BitSet[] g : got) for (int p = 0; p < PRODUCERS; p++) g[p] = new BitSet(); }

        @Override public void onMessage(int shard, long seq, long producer, long pseq, long ts, ByteBuffer b,
                                        int topicOff, int topicLen, int off, int len) {
            int p = (int) producer - 1;
            if (b.getLong(off) != pseq || b.getInt(off + 8) != shard) corrupt++;
            if (got[shard][p].get((int) pseq)) dups++;
            else if (pseq <= lastPseq[shard][p]) reordered++;
            got[shard][p].set((int) pseq);
            lastPseq[shard][p] = Math.max(lastPseq[shard][p], pseq);
            digest[shard] = digest[shard] * 1_000_003 + (producer * 31 + pseq);
            messages++;
            progressNs = System.nanoTime();
        }

        @Override public void onGap(int shard, long from, long to) { gaps += to - from; }

        long missing() {
            long m = 0;
            for (int s = 0; s < shards; s++) {
                for (int p = 0; p < PRODUCERS; p++) {
                    m += acked[s][p] - got[s][p].get(1, (int) acked[s][p] + 1).cardinality();
                }
            }
            return m;
        }
    }

    private Chaos(Cluster cluster, int rate) {
        this.cluster = cluster;
        this.shards = cluster.shards;
        this.rate = rate;
        this.acked = new long[shards][PRODUCERS];
    }

    public static int run(Args a) throws Exception {
        String mode = a.str("mode", "safe");
        int seconds = a.num("seconds", 120), rate = a.num("rate", 20_000), shards = a.num("shards", 2);
        List<String> extra = new ArrayList<>(List.of("--seg-mb", "8", "--segments", "200"));
        if (mode.equals("naive")) extra.addAll(List.of("--ack", "leader"));
        if (mode.equals("nodedup")) extra.addAll(List.of("--dedup", "off"));
        Path dir = Path.of(a.str("dir", "work/chaos-" + mode));
        try (Cluster c = new Cluster(dir, shards, List.of("-Xmx64m", "-XX:MaxDirectMemorySize=128m"), extra)) {
            return new Chaos(c, rate).go(mode, seconds, a.str("out", "results/local"));
        }
    }

    private int go(String mode, int seconds, String out) throws Exception {
        cluster.start(0);
        cluster.start(1);
        if (cluster.awaitInSync(15_000) < 0) throw new IllegalStateException("cluster did not form, see " + cluster.dir);

        Seen s1 = new Seen(), s2 = new Seen();
        Thread pub = Thread.ofPlatform().name("pub").start(this::publish);
        Thread t1 = Thread.ofPlatform().start(() -> subscribe(s1));
        Thread t2 = Thread.ofPlatform().start(() -> subscribe(s2));
        Thread.sleep(1500);

        boolean linux = !System.getProperty("os.name").startsWith("Windows");
        List<String> plan = new ArrayList<>(List.of("kill-leader", "kill-standby", "kill-leader-during-catch-up", "kill-leader"));
        if (linux) plan.add(1, "pause-leader");
        plan.add("kill-witness");
        int[] kills = new int[1];
        List<Double> failover = new ArrayList<>(), pauses = new ArrayList<>(), stalls = new ArrayList<>();
        long end = System.nanoTime() + seconds * 1_000_000_000L;
        for (int i = 0; System.nanoTime() < end; i++) {
            String f = plan.get(i % plan.size());
            int l = cluster.leader();
            if (l < 0) { System.out.println("no leader, waiting"); Thread.sleep(200); continue; }
            longestStallNs = 0;
            long t0 = System.nanoTime();
            switch (f) {
                case "kill-leader" -> {
                    cluster.kill9(l);
                    kills[0]++;
                    Thread.sleep(1000);
                    cluster.start(l);
                    Thread.sleep(500);
                    failover.add(longestStallNs / 1e6);
                }
                case "kill-standby" -> {
                    cluster.kill9(1 - l);
                    kills[0]++;
                    Thread.sleep(1000);
                    cluster.start(1 - l);
                    Thread.sleep(500);
                    stalls.add(longestStallNs / 1e6);
                }
                case "pause-leader" -> {        // a zombie: it wakes up still believing it leads
                    cluster.signal(l, "STOP");
                    Thread.sleep(2000);
                    cluster.signal(l, "CONT");
                    Thread.sleep(500);
                    pauses.add(longestStallNs / 1e6);
                }
                case "kill-leader-during-catch-up" -> {
                    // The standby is behind when the leader dies. The right answer is to wait for the old leader.
                    cluster.kill9(1 - l);
                    Thread.sleep(1500);
                    cluster.kill9(l);
                    kills[0] += 2;
                    cluster.start(1 - l);
                    Thread.sleep(2500);
                    cluster.start(l);
                    Thread.sleep(1000);
                }
                case "kill-witness" -> {        // must not interrupt traffic
                    cluster.killWitness();
                    kills[0]++;
                    Thread.sleep(1500);
                    cluster.startWitness();
                    Thread.sleep(500);
                    System.out.printf("  stall while the witness was down: %.1f ms%n", longestStallNs / 1e6);
                }
                default -> throw new AssertionError(f);
            }
            System.out.printf("%5.1fs %-28s leader was %s, longest ack stall %.0f ms, acked %d%n",
                    (System.nanoTime() - t0) / 1e9, f, Cluster.name(l), longestStallNs / 1e6, ackedTotal);
            int n = cluster.awaitInSync(20_000);
            if (n < 0) System.out.println("  not in sync after 20 s");
            Thread.sleep(700);
        }

        stopPublishing = true;
        pub.join();
        long idle = System.nanoTime();
        while (s1.missing() + s2.missing() > 0 && System.nanoTime() - Math.max(idle, Math.max(s1.progressNs, s2.progressNs)) < 4_000_000_000L) Thread.sleep(50);
        Thread.sleep(500);
        stopAll = true;
        t1.join();
        t2.join();

        double gaps = 0, trunc = 0;
        for (int i = 0; i < 2; i++) {
            gaps += Math.max(0, cluster.metric(i, "nanobus_producer_gaps_total"));
            trunc += Math.max(0, cluster.metric(i, "nanobus_truncated_committed_total"));
        }
        cluster.awaitInSync(5000);
        Thread.sleep(300);
        cluster.stopGracefully(1 - Math.max(0, cluster.leader()));
        cluster.stopGracefully(0);
        cluster.stopGracefully(1);
        String diff = JournalDiff.compare(cluster.dir.resolve("A"), cluster.dir.resolve("B"), shards);

        long lost = s1.missing(), lost2 = s2.missing();
        boolean same = true;
        for (int i = 0; i < shards; i++) same &= s1.digest[i] == s2.digest[i];
        String r = String.format(Locale.ROOT, """
                {"mode":"%s","seconds":%d,"rate":%d,"kills":%d,"pauses":%d,"acked":%d,"resent_frames_bytes":%d,
                 "lost":[%d,%d],"duplicated":[%d,%d],"reordered":[%d,%d],"corrupt":%d,"subscribers_agree":%b,
                 "producer_gaps":%.0f,"truncated_committed":%.0f,"journals":"%s",
                 "failover_ms":%s,"paused_leader_stall_ms":%s,"standby_kill_stall_ms":%s}
                """, mode, seconds, rate, kills[0], pauses.size(), ackedTotal, resent, lost, lost2, s1.dups, s2.dups,
                s1.reordered, s2.reordered, s1.corrupt + s2.corrupt, same, gaps, trunc, diff,
                list(failover), list(pauses), list(stalls));
        System.out.println(r);
        Files.createDirectories(Path.of(out));
        Files.writeString(Path.of(out, "chaos-" + mode + ".json"), r);

        boolean clean = lost + lost2 + s1.dups + s2.dups + s1.reordered + s2.reordered + s1.corrupt + s2.corrupt == 0
                && same && diff.startsWith("identical") && trunc == 0;
        return switch (mode) {
            case "safe" -> clean ? 0 : 1;
            case "naive" -> lost + lost2 > 0 ? 0 : 2;                   // a control that cannot fail proves nothing
            default -> s1.dups + s2.dups > 0 ? 0 : 2;
        };
    }

    private static String list(List<Double> v) {
        List<Double> s = new ArrayList<>(v);
        Collections.sort(s);
        StringBuilder b = new StringBuilder("[");
        for (int i = 0; i < s.size(); i++) b.append(i == 0 ? "" : ",").append(Math.round(s.get(i)));
        return b.append("]").toString();
    }

    private void publish() {
        Publisher[] p = new Publisher[PRODUCERS];
        for (int i = 0; i < PRODUCERS; i++) p[i] = new Publisher(Cluster.BROKERS, shards, i + 1);
        byte[] topic = "prices.EURUSD".getBytes();
        byte[] payload = new byte[100];
        ByteBuffer pb = ByteBuffer.wrap(payload);
        boolean up;
        do { up = true; for (Publisher x : p) { x.poll(); up &= x.ready(); } } while (!up);
        long start = System.nanoTime(), n = 0, lastAcked = 0;
        lastAckProgressNs = start;
        long drainUntil = 0;
        while (true) {
            long now = System.nanoTime();
            if (!stopPublishing) {
                // After a stall, catch up gradually instead of replaying the whole missed schedule at once.
                long due = (long) ((now - start) / 1e9 * rate);
                for (int k = 0; n < due && k < 256; k++) {
                    int pr = (int) (n % PRODUCERS), sh = (int) (n / PRODUCERS % shards);
                    pb.putLong(0, p[pr].offered(sh) + 1).putInt(8, sh);
                    if (p[pr].offer(sh, topic, payload, payload.length, now) == 0) { start += 1_000_000; break; }
                    n++;
                }
            } else if (drainUntil == 0) {
                drainUntil = now + 10_000_000_000L;
            }
            for (Publisher x : p) x.poll();
            long a = 0, o = 0;
            for (int i = 0; i < PRODUCERS; i++) for (int s = 0; s < shards; s++) { a += p[i].acked(s); o += p[i].offered(s); }
            if (a > lastAcked) {
                longestStallNs = Math.max(longestStallNs, now - lastAckProgressNs);
                lastAcked = a;
                lastAckProgressNs = now;
            }
            ackedTotal = a;
            offeredTotal = o;
            if (drainUntil != 0 && (a == o || now > drainUntil)) break;
            LockSupport.parkNanos(20_000);
        }
        long rs = 0;
        for (int i = 0; i < PRODUCERS; i++) {
            for (int s = 0; s < shards; s++) acked[s][i] = p[i].acked(s);
            rs += p[i].resent();
            p[i].close();
        }
        resent = rs;
        if (ackedTotal != offeredTotal) System.out.println("WARNING: " + (offeredTotal - ackedTotal) + " messages never acked");
    }

    private void subscribe(Seen s) {
        try (Subscriber sub = new Subscriber(Cluster.BROKERS, shards, 1, null, s)) {
            while (!stopAll) if (!sub.poll()) LockSupport.parkNanos(50_000);
        }
    }
}
