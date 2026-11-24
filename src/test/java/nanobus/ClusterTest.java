package nanobus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/** Two brokers and a witness in one JVM, over real sockets. kill -9 lives in the chaos harness. */
class ClusterTest {
    private static final byte[] T = "t".getBytes();
    private static int nextPort = 21000 + (int) (ProcessHandle.current().pid() % 2000) * 10;

    @TempDir Path dir;
    private Witness witness;
    private int wPort, aPort, bPort;
    private Broker a, b;
    private final List<AutoCloseable> open = new ArrayList<>();

    @BeforeEach void setUp() throws Exception {
        aPort = ports(4);
        bPort = ports(4);
        witness = new Witness(0, dir.resolve("w"), 300);
        wPort = witness.port();
    }

    @AfterEach void tearDown() throws Exception {
        for (AutoCloseable c : open) c.close();
        if (a != null) a.close();
        if (b != null) b.close();
        witness.close();
    }

    private static int ports(int n) throws IOException {
        while (true) {
            int p = nextPort;
            nextPort += n;
            boolean ok = true;
            for (int i = 0; i < n && ok; i++) {
                try (ServerSocket s = new ServerSocket(p + i, 1, InetAddress.getByName("127.0.0.1"))) {
                    s.setReuseAddress(true);
                } catch (IOException e) {
                    ok = false;
                }
            }
            if (ok) return p;
        }
    }

    private BrokerConfig cfg(String id) {
        BrokerConfig c = new BrokerConfig();
        c.id = id;
        c.peerId = id.equals("A") ? "B" : "A";
        c.port = id.equals("A") ? aPort : bPort;
        c.peerPort = id.equals("A") ? bPort : aPort;
        c.witnessPort = wPort;
        c.dir = dir.resolve(id);
        c.segMb = 1;
        c.maxSegments = 1000;
        c.replTimeoutMs = 300;
        return c;
    }

    private String brokers() { return "127.0.0.1:" + aPort + ",127.0.0.1:" + bPort; }

    private static void await(String what, BooleanSupplier ok) throws InterruptedException {
        long end = System.nanoTime() + 15_000_000_000L;
        while (!ok.getAsBoolean()) {
            if (System.nanoTime() > end) fail("timed out waiting for: " + what);
            Thread.sleep(2);
        }
    }

    private void startBoth() throws Exception {
        a = new Broker(cfg("A"));
        await("A leads", a::isLeader);
        b = new Broker(cfg("B"));
        await("in sync", a::standbyInSync);
    }

    private Publisher publisher(long id) throws InterruptedException {
        Publisher p = new Publisher(brokers(), 2, id);
        open.add(p);
        await("publisher ready", () -> { p.poll(); return p.ready(); });
        return p;
    }

    private static void send(Publisher p, int shard, byte[] topic, int n, int size) throws InterruptedException {
        for (int i = 0; i < n; i++) await("window", () -> { p.poll(); return p.offer(shard, topic, new byte[size], size, 0) > 0; });
    }

    private static void drain(Publisher p) throws InterruptedException {
        await("acks", () -> { p.poll(); return p.acked(0) == p.offered(0) && p.acked(1) == p.offered(1); });
    }

    /** Collects "shard:producer:pseq". */
    private final class Sink implements Subscriber.Handler {
        final List<String> got = new ArrayList<>();
        final Subscriber sub;
        long gap;
        Sink(long from, byte[] topic) { sub = new Subscriber(brokers(), 2, from, topic, this); open.add(sub); }
        @Override public void onMessage(int sh, long seq, long pr, long ps, long ts, ByteBuffer bb, int to, int tl, int po, int pl) {
            got.add(sh + ":" + pr + ":" + ps);
        }
        @Override public void onGap(int sh, long from, long to) { gap += to - from; }
        void until(int n) throws InterruptedException { await(n + " messages", () -> { sub.poll(); return got.size() >= n; }); }
    }

    private static void assertExactly(List<String> got, int shard, long producer, int n) {
        List<String> want = new ArrayList<>(), mine = new ArrayList<>();
        for (int i = 1; i <= n; i++) want.add(shard + ":" + producer + ":" + i);
        for (String g : got) if (g.startsWith(shard + ":" + producer + ":")) mine.add(g);
        assertEquals(want, mine);
    }

    @Test void deliversInOrderAndFailsOver() throws Exception {
        startBoth();
        Publisher p = publisher(1);
        Sink s = new Sink(1, null);
        send(p, 0, T, 2000, 100);
        send(p, 1, T, 1000, 100);
        drain(p);
        a.close();
        a = null;
        await("B leads", () -> b.isLeader());
        send(p, 0, T, 2000, 100);
        drain(p);
        s.until(5000);
        assertExactly(s.got, 0, 1, 4000);
        assertExactly(s.got, 1, 1, 1000);
        a = new Broker(cfg("A"));
        await("old leader is back in sync", b::standbyInSync);
        b.close();
        b = null;
        await("A leads again", a::isLeader);
        Sink late = new Sink(1, null);
        late.until(5000);
        assertExactly(late.got, 0, 1, 4000);
    }

    @Test void retriesAreDroppedEvenAfterFailover() throws Exception {
        startBoth();
        Publisher p = publisher(1);
        send(p, 0, T, 500, 50);
        drain(p);
        a.close();
        a = null;
        await("B leads", () -> b.isLeader());
        // A client that never saw its acks: same producer id, sends 1..600 again by hand.
        try (Socket sock = new Socket("127.0.0.1", bPort)) {
            ByteBuffer f = ByteBuffer.allocate(100_000);
            f.putInt(8).put(Proto.HELLO_PUB).putLong(1);
            for (int i = 1; i <= 600; i++) f.putInt(19).put(Proto.PUBLISH).putLong(i).putLong(0).putShort((short) 1).put((byte) 't');
            sock.getOutputStream().write(f.array(), 0, f.position());
            Sink s = new Sink(1, null);
            s.until(600);
            Thread.sleep(100);
            s.sub.poll();
            assertExactly(s.got, 0, 1, 600);
        }
        assertEquals(500, b.shards[0].mDups);
    }

    @Test void aStaleStandbyIsNeverPromoted() throws Exception {
        startBoth();
        Publisher p = publisher(1);
        send(p, 0, T, 100, 50);
        drain(p);
        b.close();                                    // the standby goes away ...
        b = null;
        send(p, 0, T, 100, 50);
        drain(p);                                     // ... and the leader commits 100 more without it
        assertFalse(a.standbyInSync());
        a.close();
        a = null;
        b = new Broker(cfg("B"));                     // only the stale node is up
        Thread.sleep(1200);
        assertFalse(b.isLeader(), "B misses 100 acked messages");
        a = new Broker(cfg("A"));
        await("A leads", a::isLeader);
        Sink s = new Sink(1, null);
        s.until(200);
        assertExactly(s.got, 0, 1, 200);
    }

    @Test void aDivergentTailIsCutWhenTheOldLeaderRejoins() throws Exception {
        startBoth();
        Publisher p = publisher(1);
        send(p, 0, T, 100, 50);
        drain(p);
        int epoch = a.role.epoch();
        a.close();
        a = null;
        // As if the old leader had appended 30 more records that never reached the standby.
        try (Journal j = new Journal(dir.resolve("A/shard0"), 1 << 20, 1000, false)) {
            ByteBuffer body = ByteBuffer.allocateDirect(50);
            for (int i = 1; i <= 30; i++) j.append(epoch, 1, 100 + i, 0, body, 0, 1, 50);
        }
        await("B leads", () -> b.isLeader());
        send(p, 0, T, 200, 80);
        drain(p);
        a = new Broker(cfg("A"));
        await("A is back in sync", b::standbyInSync);
        a.close();
        b.close();
        a = b = null;
        assertEquals("identical, 300 records", JournalDiff.compare(dir.resolve("A"), dir.resolve("B"), 2));
    }

    @Test void aStandbyThatHasNotCaughtUpIsNotInSync() throws Exception {
        a = new Broker(cfg("A"));
        await("A leads", a::isLeader);
        Publisher p = publisher(1);
        send(p, 0, T, 1000, 50);
        drain(p);
        List<Socket> fake = new ArrayList<>();
        for (int i = 0; i < 2; i++) {                 // says hello on both shards and then acks only seq 1
            Socket s = new Socket("127.0.0.1", aPort + i);
            fake.add(s);
            s.getOutputStream().write(ByteBuffer.allocate(29).putInt(24).put(Proto.REPL_HELLO)
                    .putInt(0).putLong(1).putLong(0).putInt(0).array());
            s.getOutputStream().write(ByteBuffer.allocate(13).putInt(8).put(Proto.REPL_ACK).putLong(i == 0 ? 1 : 0).array());
        }
        long until = System.nanoTime() + 1_000_000_000L;
        while (System.nanoTime() < until) assertFalse(a.standbyInSync());
        for (Socket s : fake) s.close();
        send(p, 0, T, 100, 50);
        drain(p);                                     // and it does not block the leader for ever
    }

    @Test void nothingIsAckedOrDeliveredBeforeTheStandbyHasIt() throws Exception {
        startBoth();
        Publisher p = publisher(1);
        Sink s = new Sink(1, null);
        send(p, 0, T, 10, 50);
        drain(p);
        s.until(10);
        witness.close();                              // the leader cannot shrink the set any more
        witness.dropClients();
        b.close();
        b = null;
        send(p, 0, T, 10, 50);
        long until = System.nanoTime() + 1_500_000_000L;
        while (System.nanoTime() < until) { p.poll(); s.sub.poll(); }
        assertEquals(10, p.acked(0));
        assertEquals(10, s.got.size());
    }

    @Test void topicFilterAndLiveTail() throws Exception {
        startBoth();
        Publisher p = publisher(1);
        for (int i = 0; i < 300; i++) send(p, 0, (i % 3 == 0 ? "md.A" : "md.B").getBytes(), 1, 50);
        drain(p);
        Sink s = new Sink(1, "md.A".getBytes());
        s.until(100);
        assertEquals("0:1:1", s.got.get(0));
        assertEquals("0:1:298", s.got.get(99));
        Sink live = new Sink(-1, null);
        await("ready", () -> { live.sub.poll(); return live.sub.ready(); });
        send(p, 0, T, 5, 10);
        drain(p);
        live.until(5);
        assertEquals("0:1:301", live.got.get(0));
    }

    @Test void aSlowConsumerReadsFromDiskAndDoesNotSlowTheOthers() throws Exception {
        startBoth();
        Publisher p = publisher(1);
        Sink slow = new Sink(1, null);
        await("ready", () -> { slow.sub.poll(); return slow.sub.ready(); });
        Sink fast = new Sink(1, null);
        for (int i = 0; i < 50; i++) { send(p, 0, T, 200, 1000); drain(p); fast.sub.poll(); }      // ~10 MB, 10 segments
        fast.until(10_000);
        assertTrue(slow.got.size() < 2000, "it has not polled");
        slow.until(10_000);
        assertExactly(slow.got, 0, 1, 10_000);
    }

    @Test void retentionShowsUpAsAGapNotAsSilence() throws Exception {
        BrokerConfig c = cfg("A");
        c.maxSegments = 3;
        a = new Broker(c);
        await("A leads", a::isLeader);
        Publisher p = publisher(1);
        for (int i = 0; i < 50; i++) { send(p, 0, T, 200, 1000); drain(p); }
        Sink s = new Sink(1, null);
        await("caught up", () -> { s.sub.poll(); return s.sub.nextSeq(0) == 10_001; });
        assertTrue(s.gap > 5000);
        assertEquals(10_000, s.got.size() + s.gap);
    }

    @Test void aStandbyThatIsTooFarBehindIsReset() throws Exception {
        BrokerConfig c = cfg("A");
        c.maxSegments = 3;
        a = new Broker(c);
        await("A leads", a::isLeader);
        Publisher first = publisher(5);
        send(first, 0, T, 1, 10);                     // only in a segment that will be deleted
        drain(first);
        Publisher p = publisher(1);
        for (int i = 0; i < 50; i++) { send(p, 0, T, 200, 1000); drain(p); }
        BrokerConfig cb = cfg("B");
        cb.maxSegments = 3;
        b = new Broker(cb);
        await("in sync", a::standbyInSync);
        a.close();
        a = null;
        await("B leads", () -> b.isLeader());
        send(p, 0, T, 10, 50);
        drain(p);
        assertEquals(1, b.shards[0].journal.producers.get(5).lastPseq);
        Sink s = new Sink(1, null);
        await("caught up", () -> { s.sub.poll(); return s.sub.nextSeq(0) == 10_012; });
    }

    @Test void garbageAndOversizedFramesOnlyCloseTheirOwnConnection() throws Exception {
        startBoth();
        Publisher p = publisher(1);
        byte[][] bad = {
                {0x7f, 0, 0, 0, 1},                                    // 2 GB frame
                {-1, -1, -1, -1, 1},                                   // negative length
                {0, 0, 0, 1, 99, 0},                                   // unknown type
                {0, 0, 0, 3, Proto.PUBLISH, 1, 2, 3},                  // too short, and no HELLO
                {0, 0, 0, 2, Proto.HELLO_PUB, 1, 2},                   // too short
                {0, 0, 0, 0, Proto.SUBSCRIBE},
                {0, 0, 0, 4, Proto.REPL_HELLO, 0, 0, 0, 0},
                {0, 0, 0, 1, Proto.REPL_ACK, 0},
        };
        for (byte[] x : bad) {
            try (Socket s = new Socket("127.0.0.1", aPort)) {
                s.setSoTimeout(3000);
                s.getOutputStream().write(x);
                int r;
                try { r = s.getInputStream().read(); } catch (java.net.SocketException e) { r = -1; }
                assertEquals(-1, r, "connection must be closed");
            }
        }
        java.util.Random r = new java.util.Random(1);
        for (int i = 0; i < 300; i++) {                                // plausible headers, random bodies
            byte[] x = new byte[5 + r.nextInt(60)];
            r.nextBytes(x);
            ByteBuffer.wrap(x).putInt(0, x.length - 5).put(4, (byte) (i % 2 == 0 ? 1 + r.nextInt(3) : 20 + r.nextInt(2)));
            try (Socket s = new Socket("127.0.0.1", aPort)) { s.getOutputStream().write(x); }
        }
        send(p, 0, T, 100, 50);
        drain(p);
        assertTrue(a.isLeader());
    }
}
