package nanobus;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static nanobus.Proto.*;

/**
 * One shard = one thread = one port = one journal. Shards share nothing, so there are no locks on the
 * message path; scaling out means more shards. Each turn of the event loop:
 *
 *   1. read every ready socket; PUBLISH frames are appended to the journal buffer
 *   2. send the new records to the standby
 *   3. flush() the journal: one write() for the whole batch
 *   4. advance the commit index (local flush AND standby ack)
 *   5. ack publishers and fan out, both only up to the commit index
 *
 * Batching comes from the loop itself: the busier it is, the more each turn picks up.
 */
final class Shard implements Runnable {
    /** What the broker's control thread wants this shard to be. Compared by identity. */
    record Role(Kind kind, int epoch) {}
    enum Kind { STANDBY, FENCED, LEADER }

    private static final int CHUNK = 128 * 1024;
    private static final long CONNECT_RETRY_NS = 50_000_000L;

    final int index;
    private final BrokerConfig cfg;
    private final Broker broker;
    private final Selector selector;
    private final ServerSocketChannel server;
    final Journal journal;
    final CopyOnWriteArrayList<Conn> conns = new CopyOnWriteArrayList<>();
    private final ArrayDeque<ByteBuffer> pool = new ArrayDeque<>();
    private final Consumer<SelectionKey> onKey = this::onKey;
    private final Map<Long, Conn> sessionsById = new ConcurrentHashMap<>();   // only used with --plant-leak
    private long nextConnId = 1;

    private Role role;
    volatile Role applied;
    private long now;
    private long commit;
    private Conn upstream;                 // standby: connection to the leader
    private long nextConnectNs;
    private boolean moreWork;
    private long lastHeartbeatNs;

    // leader: state of the standby
    private Conn replica;
    private boolean syncRequired;          // commit waits for the standby
    private long syncPoint, replicaGoneNs;
    volatile boolean mSynced;              // standby has everything up to the commit index, and commit waits for it
    volatile boolean mLost;                // commit waits for a standby that is gone or stuck
    volatile boolean release;              // control thread: the witness has removed the standby, stop waiting

    // heap-cache experiment
    private record Msg(long seq, byte[] bytes) {}
    private final Msg[] cache;

    // metrics: written once per loop turn by this thread, read by the metrics thread
    private final TopicTable topics = new TopicTable();
    private long published, dups, delivered, pubGaps, truncatedCommitted, bytesIn;
    volatile long mPublished, mDups, mDelivered, mPubGaps, mTruncatedCommitted, mBytesIn;
    volatile long mCommit, mLast, mReplicaAck;
    volatile boolean mReplicaUp;
    volatile long mLoops;

    Shard(int index, BrokerConfig cfg, Broker broker) throws IOException {
        this.index = index;
        this.cfg = cfg;
        this.broker = broker;
        this.journal = new Journal(cfg.dir.resolve("shard" + index), cfg.segMb << 20, cfg.maxSegments, cfg.fsync);
        this.cache = cfg.heapCache > 0 ? new Msg[cfg.heapCache] : null;
        this.selector = Selector.open();
        this.server = ServerSocketChannel.open();
        server.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        server.bind(new InetSocketAddress(cfg.host, cfg.port + index), 1024);
        server.configureBlocking(false);
        server.register(selector, SelectionKey.OP_ACCEPT);
        this.role = new Role(Kind.FENCED, 0);
        this.applied = role;
        if (journal.tornBytes > 0) Broker.log(cfg.id, "shard " + index + " dropped a torn tail of " + journal.tornBytes + " bytes");
    }

    void wakeup() { selector.wakeup(); }
    TopicTable topics() { return topics; }

    @Override public void run() {
        try {
            while (!broker.stopping) {
                if (moreWork) selector.selectNow(onKey); else selector.select(onKey, 10);
                moreWork = false;
                now = System.nanoTime();
                Role want = broker.role;
                if (want != role) apply(want);
                if (role.kind == Kind.LEADER) leaderTurn();
                else if (role.kind == Kind.STANDBY) standbyTurn();
                publishMetrics();
            }
            closeAll();
            server.close();
            selector.close();        // a closed channel keeps its port until the selector lets go of the key
            journal.close();
        } catch (Throwable t) {
            broker.fatal("shard " + index, t);
        }
    }

    private void apply(Role want) throws IOException {
        closeAll();
        replica = null;
        syncRequired = false;
        mSynced = mLost = release = false;
        role = want;
        if (want.kind == Kind.LEADER) {
            // The witness just shrank the eligible set to this node, so its whole log is committed.
            journal.flush();
            commit = journal.lastSeq();
        }
        applied = want;
    }

    private void closeAll() {
        for (Conn c : conns) close(c);
        upstream = null;
    }

    // ---------------------------------------------------------------- leader

    private void leaderTurn() throws IOException {
        if (release) {
            syncRequired = false;
            mSynced = false;
            mLost = false;
            if (replica != null && replica.ackSeq < journal.lastSeq() && now - replica.lastAckNs > replTimeoutNs()) close(replica);
            release = false;
        }
        boolean beat = now - lastHeartbeatNs > HEARTBEAT_MS * 1_000_000L;
        if (beat) lastHeartbeatNs = now;

        if (replica != null) pump(replica, journal.lastSeq());
        journal.flush();

        long flushed = journal.lastSeq();
        long newCommit = cfg.ackAll && syncRequired ? Math.min(flushed, replica == null ? 0 : replica.ackSeq) : flushed;
        if (newCommit > commit) commit = newCommit;
        trackReplica(flushed);

        for (int i = 0, n = conns.size(); i < n; i++) {
            Conn c = conns.get(i);
            if (c.closed) continue;
            try {
                if (c.kind == Conn.PUB) {
                    if (c.gotPublish) {
                        c.gotPublish = false;
                        c.pushAck(c.producer.lastLogSeq, c.producer.lastPseq);
                    }
                    long a = c.popAck(commit);
                    if (a >= 0 && c.out.remaining() >= 13) {
                        frame(c.out, PUB_ACK, 8);
                        c.out.putLong(a);
                    }
                } else if (c.kind == Conn.SUB) {
                    pump(c, commit);
                    c.mNextSeq = c.cursor.seq;
                }
                if (beat && c.kind != Conn.NEW && c.out.remaining() >= 13) {
                    frame(c.out, HEARTBEAT, 8);
                    c.out.putLong(commit);
                }
                c.flush();
                if (c.closeAfterFlush && c.out.position() == 0) close(c);
                // More to send and the socket took everything: no OP_WRITE will come, so do not sleep.
                if (c.cursor != null && c.out.position() == 0 && c.cursor.seq <= (c == replica ? flushed : commit)) moreWork = true;
            } catch (IOException e) {
                close(c);
            }
        }
        sweepClosed();
    }

    private long replTimeoutNs() { return cfg.replTimeoutMs * 1_000_000L; }

    private void trackReplica(long flushed) {
        if (replica != null && replica.ackSeq >= flushed) replica.lastAckNs = now;   // nothing is owed
        if (!syncRequired) {
            // Start waiting for the standby once it is nearly caught up; it counts as in sync only when it
            // also holds everything that was committed without it.
            if (replica != null && flushed - replica.ackSeq < 20_000 && now - replica.lastAckNs < replTimeoutNs()) {
                syncRequired = true;
                syncPoint = commit;
            }
        } else if (!mSynced && replica != null && replica.ackSeq >= syncPoint) {
            mSynced = true;
        }
        boolean lost = false;
        if (syncRequired) {
            if (replica == null) lost = now - replicaGoneNs > replTimeoutNs();
            else lost = replica.ackSeq < flushed && now - replica.lastAckNs > replTimeoutNs();
        }
        if (mLost != lost) mLost = lost;
    }

    /** Moves records from the journal to a subscriber or to the standby, up to limit. */
    private void pump(Conn c, long limit) throws IOException {
        if (c.cursor == null || c.closed) return;
        if (c.cursor.seq < journal.firstSeq()) {
            if (c.kind == Conn.REPLICA) { close(c); return; }      // it will reconnect and be reset
            if (c.out.remaining() < 13) return;
            frame(c.out, GAP, 8);
            c.out.putLong(journal.firstSeq());
            c.cursor.seek(journal.firstSeq());
        }
        if (c.cursor.seq > limit) return;
        int hdr = c.kind == Conn.REPLICA ? FRAME_HDR + 8 : FRAME_HDR;
        if (c.out.remaining() < hdr + MAX_RECORD) return;          // OP_WRITE will bring us back
        int at = c.out.position();
        c.out.position(at + hdr);
        int max = CHUNK - hdr;
        int n;
        if (c.kind == Conn.SUB && cache != null && c.topic == null && inCache(c.cursor.seq)) n = readCache(c, limit, max);
        else n = journal.read(c.cursor, limit, c.out, max, c.topic);
        if (n == 0) {
            c.out.position(at);
        } else {
            c.out.putInt(at, hdr - FRAME_HDR + n);
            c.out.put(at + 4, c.kind == Conn.REPLICA ? REPL_DATA : DATA);
            if (c.kind == Conn.REPLICA) c.out.putLong(at + FRAME_HDR, commit);
            delivered += n;
        }
        if (c.kind == Conn.REPLICA) {
            try { c.flush(); } catch (IOException e) { close(c); }
        }
    }

    private boolean inCache(long seq) {
        Msg m = cache[(int) (seq % cache.length)];
        return m != null && m.seq == seq;
    }

    private int readCache(Conn c, long limit, int max) {
        int n = 0;
        while (c.cursor.seq <= limit && inCache(c.cursor.seq)) {
            byte[] b = cache[(int) (c.cursor.seq % cache.length)].bytes;
            if (n + b.length > max || b.length > c.out.remaining()) break;
            c.out.put(b);
            n += b.length;
            c.cursor.seek(c.cursor.seq + 1);
        }
        return n;
    }

    // ---------------------------------------------------------------- standby

    private void standbyTurn() throws IOException {
        if (upstream == null) {
            if (cfg.peerPort != 0 && now - nextConnectNs >= 0) connectUpstream();
        } else if (now - upstream.lastReadNs > replTimeoutNs()) {
            close(upstream);
        }
        for (int i = 0, n = conns.size(); i < n; i++) {
            Conn c = conns.get(i);
            if (c.closed || !c.ch.isConnected()) continue;
            try {
                c.flush();
                if (c.closeAfterFlush && c.out.position() == 0) close(c);
            } catch (IOException e) {
                close(c);
            }
        }
        sweepClosed();
    }

    private void connectUpstream() throws IOException {
        nextConnectNs = now + CONNECT_RETRY_NS;
        SocketChannel ch = SocketChannel.open();
        ch.configureBlocking(false);
        Conn c = newConn(ch);
        c.kind = Conn.UPSTREAM;
        upstream = c;
        try {
            boolean done = ch.connect(new InetSocketAddress(cfg.peerHost, cfg.peerPort + index));
            c.key = ch.register(selector, SelectionKey.OP_CONNECT, c);
            if (done) finishConnect(c);
        } catch (IOException e) {
            close(c);
        }
    }

    private void finishConnect(Conn c) throws IOException {
        if (!c.ch.finishConnect()) return;
        c.ch.setOption(StandardSocketOptions.TCP_NODELAY, true);
        c.key.interestOps(SelectionKey.OP_READ);
        c.lastReadNs = now;
        int n = journal.epochs.size();
        frame(c.out, REPL_HELLO, 24 + 12 * n);
        c.out.putInt(broker.promised()).putLong(journal.firstSeq()).putLong(journal.lastSeq()).putInt(n);
        for (long[] e : journal.epochs) c.out.putInt((int) e[0]).putLong(e[1]);
        c.flush();
    }

    private void onUpstreamFrame(Conn c, byte type, ByteBuffer b, int p, int len) throws IOException {
        switch (type) {
            case REPL_START -> {
                int epoch = b.getInt(p);
                if (!broker.promise(epoch)) { close(c); return; }      // an older leader that does not know it lost
                long keep = b.getLong(p + 4);
                int snapLen = b.getInt(p + 12);
                if (keep < Math.min(commit, journal.lastSeq())) truncatedCommitted++;
                if (snapLen > 0) {
                    byte[] snap = new byte[snapLen];
                    b.get(p + 16, snap);
                    journal.reset(keep + 1, snap);
                } else if (keep < journal.lastSeq()) {
                    Broker.log(cfg.id, "shard " + index + " truncating " + (journal.lastSeq() - keep) + " uncommitted records");
                    journal.truncateAfter(keep);
                }
                sendReplAck(c);
            }
            case REPL_DATA -> {
                long leaderCommit = b.getLong(p);
                int end = p + len;
                for (int q = p + 8; q < end; ) q += journal.appendRaw(b, q);
                journal.flush();
                commit = Math.min(leaderCommit, journal.lastSeq());
                sendReplAck(c);
            }
            case HEARTBEAT -> commit = Math.min(b.getLong(p), journal.lastSeq());
            default -> close(c);                                        // NOT_LEADER: retry later
        }
    }

    private void sendReplAck(Conn c) {
        if (c.out.remaining() < 13) return;                             // a later ack covers this one
        frame(c.out, REPL_ACK, 8);
        c.out.putLong(journal.lastSeq());
    }

    // ---------------------------------------------------------------- sockets

    private void onKey(SelectionKey key) {
        now = System.nanoTime();
        if (!key.isValid()) return;
        if (key.isAcceptable()) { accept(); return; }
        Conn c = (Conn) key.attachment();
        if (c.closed) return;
        try {
            if (key.isConnectable()) { finishConnect(c); return; }
            if (key.isWritable()) c.flush();
            if (key.isReadable()) read(c);
        } catch (IOException e) {
            close(c);
        }
    }

    private void accept() {
        try {
            for (SocketChannel ch; (ch = server.accept()) != null; ) {
                ch.configureBlocking(false);
                ch.setOption(StandardSocketOptions.TCP_NODELAY, true);
                Conn c = newConn(ch);
                c.key = ch.register(selector, SelectionKey.OP_READ, c);
            }
        } catch (IOException e) {
            Broker.log(cfg.id, "accept: " + e);
        }
    }

    private Conn newConn(SocketChannel ch) {
        Conn c = new Conn(nextConnId++, ch, buffer(), buffer());
        c.lastReadNs = now;
        conns.add(c);
        return c;
    }

    private ByteBuffer buffer() {
        ByteBuffer b = pool.poll();
        return b != null ? b.clear() : ByteBuffer.allocateDirect(BUF);
    }

    private void close(Conn c) {
        if (c.closed) return;
        c.closed = true;
        try { c.ch.close(); } catch (IOException ignored) { /* nothing to do */ }
        if (c == replica) { replica = null; replicaGoneNs = now; }
        if (c == upstream) { upstream = null; nextConnectNs = now + CONNECT_RETRY_NS; }
        if (cfg.plantLeak && c.kind == Conn.SUB) sessionsById.put(c.id, c);    // planted: a registry nobody cleans
        moreWork = true;                                                 // so that sweepClosed runs this turn
    }

    /** Removal is deferred so that closing never disturbs a loop over conns, or a buffer still in use. */
    private void sweepClosed() {
        for (int i = conns.size() - 1; i >= 0; i--) {
            Conn c = conns.get(i);
            if (!c.closed) continue;
            conns.remove(i);
            if (cfg.poolBuffers && !(cfg.plantLeak && c.kind == Conn.SUB) && pool.size() < 256) {
                pool.add(c.in);
                pool.add(c.out);
            }
        }
    }

    private void read(Conn c) throws IOException {
        ByteBuffer b = c.in;
        int n = c.ch.read(b);
        if (n < 0) { close(c); return; }
        c.lastReadNs = now;
        bytesIn += n;
        int p = 0, end = b.position();
        while (end - p >= FRAME_HDR && !c.closed && !c.closeAfterFlush) {
            int len = b.getInt(p);
            if (len < 0 || len > BUF - FRAME_HDR) { close(c); return; }
            if (end - p - FRAME_HDR < len) break;
            onFrame(c, b.get(p + 4), b, p + FRAME_HDR, len);
            p += FRAME_HDR + len;
        }
        if (p > 0) {
            if (p == end) b.clear();
            else b.position(p).limit(end).compact();
        }
    }

    private void onFrame(Conn c, byte type, ByteBuffer b, int p, int len) throws IOException {
        if (c.kind == Conn.UPSTREAM) { onUpstreamFrame(c, type, b, p, len); return; }
        if (role.kind != Kind.LEADER) { refuse(c); return; }
        if (len < minLen(type) || (c.kind == Conn.NEW) != isHello(type)) { close(c); return; }
        switch (type) {
            case PUBLISH -> onPublish(c, b, p, len);
            case REPL_ACK -> {
                if (c != replica) { close(c); return; }
                long a = b.getLong(p);
                if (a > c.ackSeq || a == journal.lastSeq()) c.lastAckNs = now;
                c.ackSeq = Math.max(c.ackSeq, a);
            }
            case HELLO_PUB -> {
                long id = b.getLong(p);
                for (Conn o : conns) if (o != c && o.kind == Conn.PUB && o.producer.id == id) close(o);
                c.kind = Conn.PUB;
                c.producer = journal.producers.get(id);
                if (c.producer.lastPseq > 0) c.pushAck(c.producer.lastLogSeq, c.producer.lastPseq);
                frame(c.out, HELLO_OK, 12);
                c.out.putInt(role.epoch).putLong(c.producer.lastPseq);
            }
            case SUBSCRIBE -> {
                long from = b.getLong(p);
                int tl = b.getShort(p + 8);
                if (tl < 0 || 10 + tl != len) { close(c); return; }
                if (tl > 0) { c.topic = new byte[tl]; b.get(p + 10, c.topic); }
                c.cursor = new Journal.Cursor(from < 0 ? commit + 1 : Math.max(1, from));
                c.mNextSeq = c.cursor.seq;
                c.kind = Conn.SUB;
                frame(c.out, HELLO_OK, 12);
                c.out.putInt(role.epoch).putLong(c.cursor.seq);
            }
            case REPL_HELLO -> onReplHello(c, b, p, len);
            default -> close(c);
        }
    }

    private static boolean isHello(byte t) { return t == HELLO_PUB || t == SUBSCRIBE || t == REPL_HELLO; }

    /** Everything a handler reads without looking must be inside the frame, whoever sent it. */
    private static int minLen(byte t) {
        return switch (t) {
            case HELLO_PUB, REPL_ACK -> 8;
            case SUBSCRIBE -> 10;
            case PUBLISH -> PUB_HDR + 1;
            case REPL_HELLO -> 24;
            default -> Integer.MAX_VALUE;      // unknown type
        };
    }

    private void refuse(Conn c) {
        if (c.out.remaining() >= FRAME_HDR) frame(c.out, NOT_LEADER, 0);
        c.closeAfterFlush = true;
    }

    private void onPublish(Conn c, ByteBuffer b, int p, int len) throws IOException {
        int tl = b.getShort(p + 16), body = len - PUB_HDR;
        if (c.kind != Conn.PUB || tl <= 0 || tl > body || body > MAX_BODY) { close(c); return; }
        long pseq = b.getLong(p), last = c.producer.lastPseq;
        c.gotPublish = true;
        if (cfg.dedup && pseq <= last) { dups++; return; }             // a retry of something we already have
        if (cfg.dedup && pseq > last + 1) {
            pubGaps++;                                                  // impossible unless acked messages were lost
            if (cfg.ackAll) { close(c); return; }
        }
        journal.append(role.epoch, c.producer.id, pseq, b.getLong(p + 8), b, p + PUB_HDR, tl, body);
        if (cache != null) cacheLast();
        topics.hit(b, p + PUB_HDR, tl);
        published++;
    }

    private void cacheLast() {
        long seq = journal.lastSeq();
        cache[(int) (seq % cache.length)] = new Msg(seq, journal.copyLast());
    }

    private void onReplHello(Conn c, ByteBuffer b, int p, int len) throws IOException {
        if (b.getInt(p) > role.epoch) { refuse(c); return; }           // the caller has seen a newer leader than me
        long first = b.getLong(p + 4), last = b.getLong(p + 12);
        int n = b.getInt(p + 20);
        if (n < 0 || n != (len - 24) / 12) { close(c); return; }
        long[] ep = new long[n], st = new long[n];
        for (int i = 0, q = p + 24; i < n; i++, q += 12) { ep[i] = b.getInt(q); st[i] = b.getLong(q + 4); }
        long keep = journal.commonPrefix(ep, st, n, last);
        byte[] snap = new byte[0];
        if (keep + 1 < journal.firstSeq() || keep + 1 < first) {        // the logs do not overlap: start it over
            keep = journal.firstSeq() - 1;
            snap = journal.snapshotOfOldest();
        }
        if (replica != null) close(replica);
        replica = c;
        c.kind = Conn.REPLICA;
        c.cursor = new Journal.Cursor(keep + 1);
        c.lastAckNs = now;
        frame(c.out, REPL_START, 16 + snap.length);
        c.out.putInt(role.epoch).putLong(keep).putInt(snap.length).put(snap);
    }

    private void publishMetrics() {
        mPublished = published;
        mDups = dups;
        mDelivered = delivered;
        mPubGaps = pubGaps;
        mTruncatedCommitted = truncatedCommitted;
        mBytesIn = bytesIn;
        mCommit = commit;
        mLast = journal.lastSeq();
        mReplicaUp = replica != null;
        mReplicaAck = replica == null ? 0 : replica.ackSeq;
        mLoops = mLoops + 1;
    }
}
