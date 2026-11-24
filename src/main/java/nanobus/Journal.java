package nanobus;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32C;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static nanobus.Proto.*;

/**
 * Append-only segmented log of one shard. Single-threaded: only the shard's event loop touches it.
 *
 * The active segment lives twice: in a direct buffer (readers that are caught up copy from it without
 * a syscall) and in the file (written by flush(), once per event-loop iteration). Older segments are
 * read back with positional reads, so a slow consumer reads from disk and never holds memory.
 */
final class Journal implements AutoCloseable {
    static final class Segment {
        final long base;             // seq of its first record
        final Path file;
        final FileChannel ch;
        int size;                    // valid bytes
        private int lastIdxPos = -(1 << 20);
        private long[] idx = new long[64];   // sparse index: pairs (seq, pos), one per 64 KB
        private int idxN;

        Segment(long base, Path file, FileChannel ch) { this.base = base; this.file = file; this.ch = ch; }

        void index(long seq, int pos) {
            if (pos - lastIdxPos < 64 * 1024) return;
            if (idxN * 2 == idx.length) idx = Arrays.copyOf(idx, idx.length * 2);
            idx[idxN * 2] = seq;
            idx[idxN * 2 + 1] = pos;
            idxN++;
            lastIdxPos = pos;
        }

        int posBefore(long seq) {
            int p = 0;
            for (int i = 0; i < idxN && idx[i * 2] <= seq; i++) p = (int) idx[i * 2 + 1];
            return p;
        }
    }

    /** A reader's place: the next seq it wants, and (as a cache) where that record is. */
    static final class Cursor {
        long seq;
        Segment seg;                 // null = must be located
        int pos;
        Cursor(long seq) { this.seq = seq; }
        void seek(long seq) { this.seq = seq; this.seg = null; }
    }

    final ProducerTable producers = new ProducerTable();
    final List<long[]> epochs = new ArrayList<>();   // (epoch, first seq written in it)

    private static final int MAX_SCAN = 4096;        // records visited per read()

    private final Path dir;
    private final int segBytes, maxSegments;
    private final boolean fsync;
    private final ByteBuffer buf;                    // active segment image; position = append point
    private final ByteBuffer view;                   // scratch view of buf
    private final ByteBuffer scratch = ByteBuffer.allocateDirect(MAX_RECORD * 2);
    private final CRC32C crc = new CRC32C();
    private final ArrayList<Segment> segs = new ArrayList<>();
    private Segment active;
    private int flushed, lastPos;
    private long nextSeq = 1;
    long tornBytes;                                  // dropped by crash recovery

    Journal(Path dir, int segBytes, int maxSegments, boolean fsync) throws IOException {
        this.dir = dir;
        this.segBytes = segBytes;
        this.maxSegments = maxSegments;
        this.fsync = fsync;
        this.buf = ByteBuffer.allocateDirect(segBytes);
        this.view = buf.duplicate();
        Files.createDirectories(dir);
        recover(Long.MAX_VALUE);
    }

    long lastSeq() { return nextSeq - 1; }
    long firstSeq() { return segs.get(0).base; }
    int segments() { return segs.size(); }

    // ---------------------------------------------------------------- append

    /** Leader path: builds the record around a topic+payload that is still in the socket buffer. */
    long append(int epoch, long producerId, long pseq, long ts, ByteBuffer src, int off, int topicLen, int bodyLen)
            throws IOException {
        int len = R_HDR + bodyLen;
        int p = reserve(len);
        long seq = nextSeq;
        buf.putInt(p + R_LEN, len);
        buf.putLong(p + R_SEQ, seq);
        buf.putInt(p + R_EPOCH, epoch);
        buf.putInt(p + R_TOPIC_LEN, topicLen << 16);
        buf.putLong(p + R_PRODUCER, producerId);
        buf.putLong(p + R_PSEQ, pseq);
        buf.putLong(p + R_TS, ts);
        buf.put(p + R_HDR, src, off, bodyLen);
        view.limit(p + len).position(p + 8);
        crc.reset();
        crc.update(view);
        buf.putInt(p + R_CRC, (int) crc.getValue());
        appended(p, len, seq, epoch, producerId, pseq);
        return seq;
    }

    /** Standby path: takes the leader's record bytes unchanged. Returns the record length. */
    int appendRaw(ByteBuffer src, int off) throws IOException {
        int len = src.getInt(off);
        long seq = src.getLong(off + R_SEQ);
        if (seq != nextSeq) throw new IOException("replication gap: got " + seq + " want " + nextSeq);
        int p = reserve(len);
        buf.put(p, src, off, len);
        if (!crcOk(p, len)) throw new IOException("bad crc from leader at seq " + seq);
        appended(p, len, seq, buf.getInt(p + R_EPOCH), buf.getLong(p + R_PRODUCER), buf.getLong(p + R_PSEQ));
        return len;
    }

    private int reserve(int len) throws IOException {
        if (buf.position() + len > segBytes) roll();
        return buf.position();
    }

    private void appended(int p, int len, long seq, int epoch, long producerId, long pseq) {
        active.index(seq, p);
        note(seq, epoch, producerId, pseq);
        lastPos = p;
        buf.position(p + len);
        active.size = p + len;
        nextSeq = seq + 1;
    }

    /** The newest record as a heap array (only the GC experiment wants this). */
    byte[] copyLast() {
        byte[] b = new byte[buf.getInt(lastPos)];
        buf.get(lastPos, b);
        return b;
    }

    private void note(long seq, int epoch, long producerId, long pseq) {
        if (epochs.isEmpty() || epochs.get(epochs.size() - 1)[0] < epoch) epochs.add(new long[]{epoch, seq});
        ProducerTable.State s = producers.get(producerId);
        s.lastPseq = pseq;
        s.lastLogSeq = seq;
    }

    /** One write() for everything appended since the last call. Must precede any ack. */
    void flush() throws IOException {
        int end = buf.position();
        if (flushed == end) return;
        view.limit(end).position(flushed);
        while (view.hasRemaining()) active.ch.write(view, view.position());
        flushed = end;
        if (fsync) active.ch.force(false);
    }

    private void roll() throws IOException {
        flush();
        openNew(nextSeq);
        while (segs.size() > maxSegments) deleteSegment(segs.remove(0));
    }

    private void openNew(long base) throws IOException {
        Path f = dir.resolve(String.format("%020d.log", base));
        Path snap = snapFile(base), tmp = snap.resolveSibling("snap.tmp");
        Files.write(tmp, producers.encode(epochs));
        Files.move(tmp, snap, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        active = new Segment(base, f, FileChannel.open(f, CREATE, READ, WRITE));
        segs.add(active);
        buf.clear();
        flushed = 0;
    }

    private Path snapFile(long base) { return dir.resolve(String.format("%020d.snap", base)); }

    private void deleteSegment(Segment s) throws IOException {
        s.ch.close();
        Files.deleteIfExists(s.file);
        Files.deleteIfExists(snapFile(s.base));
    }

    // ---------------------------------------------------------------- read

    /**
     * Copies whole records with seq <= limitSeq into dst, at most maxBytes, advancing the cursor.
     * With a topic filter, non-matching records are skipped. Returns bytes copied.
     */
    int read(Cursor c, long limitSeq, ByteBuffer dst, int maxBytes, byte[] topic) throws IOException {
        int start = Math.min(maxBytes, dst.remaining()), room = start;
        long stopAt = c.seq + MAX_SCAN;                      // a sparse filter must not stall the event loop
        while (c.seq <= limitSeq && c.seq < stopAt) {
            if (c.seg == null) locate(c);
            if (c.pos >= c.seg.size) {                       // finished this segment
                int i = segs.indexOf(c.seg);
                if (i < 0) { c.seg = null; continue; }       // retention deleted it behind us
                if (i + 1 >= segs.size()) break;
                c.seg = segs.get(i + 1);
                c.pos = 0;
                continue;
            }
            if (c.seg == active) {
                int len = buf.getInt(c.pos);
                if (topic == null || matches(buf, c.pos, topic)) {
                    if (len > room) break;
                    dst.put(dst.position(), buf, c.pos, len).position(dst.position() + len);
                    room -= len;
                }
                c.pos += len;
                c.seq++;
            } else {
                int n = load(c.seg, c.pos);
                int p = 0;
                boolean full = false;
                while (p + 4 <= n && c.seq <= limitSeq) {
                    int len = scratch.getInt(p);
                    if (p + len > n) break;
                    if (topic == null || matches(scratch, p, topic)) {
                        if (len > room) { full = true; break; }
                        dst.put(dst.position(), scratch, p, len).position(dst.position() + len);
                        room -= len;
                    }
                    p += len;
                    c.seq++;
                }
                c.pos += p;
                if (full) break;
            }
        }
        return start - room;
    }

    private int load(Segment s, int pos) throws IOException {
        scratch.clear();
        while (scratch.hasRemaining()) if (s.ch.read(scratch, pos + scratch.position()) <= 0) break;
        int n = Math.min(scratch.position(), s.size - pos);
        scratch.clear();
        return n;
    }

    private static boolean matches(ByteBuffer b, int p, byte[] topic) {
        if (b.getShort(p + R_TOPIC_LEN) != topic.length) return false;
        for (int i = 0; i < topic.length; i++) if (b.get(p + R_HDR + i) != topic[i]) return false;
        return true;
    }

    private void locate(Cursor c) throws IOException {
        Segment s = segs.get(0);
        for (int i = segs.size() - 1; i >= 0; i--) if (segs.get(i).base <= c.seq) { s = segs.get(i); break; }
        int pos = s.posBefore(c.seq);
        while (pos < s.size) {
            long seq;
            int len;
            if (s == active) {
                len = buf.getInt(pos);
                seq = buf.getLong(pos + R_SEQ);
            } else {
                scratch.clear().limit(16);
                while (scratch.hasRemaining()) if (s.ch.read(scratch, pos + scratch.position()) <= 0) break;
                scratch.clear();
                len = scratch.getInt(R_LEN);
                seq = scratch.getLong(R_SEQ);
            }
            if (seq >= c.seq) break;
            pos += len;
        }
        c.seg = s;
        c.pos = pos;
    }

    // ---------------------------------------------------------------- recovery and truncation

    /** Drops every record after seq: a rejoining node may hold a tail the new leader never had. */
    void truncateAfter(long seq) throws IOException {
        flush();
        recover(seq);
    }

    /** Discards everything and restarts at base with the leader's producer table. */
    void reset(long base, byte[] snap) throws IOException {
        for (Segment s : segs) deleteSegment(s);
        segs.clear();
        nextSeq = base;
        producers.decode(snap, epochs);
        openNew(base);
    }

    byte[] snapshotOfOldest() throws IOException { return Files.readAllBytes(snapFile(firstSeq())); }

    /** Rebuilds all state by scanning the files. Stops at the first bad record or after maxSeq. */
    private void recover(long maxSeq) throws IOException {
        for (Segment s : segs) s.ch.close();
        segs.clear();
        List<Path> files = new ArrayList<>();
        try (var st = Files.list(dir)) {
            st.filter(p -> p.toString().endsWith(".log")).sorted().forEach(files::add);
        }
        if (files.isEmpty()) {
            nextSeq = 1;
            producers.clear();
            epochs.clear();
            openNew(1);
            return;
        }
        producers.decode(Files.readAllBytes(snapFile(baseOf(files.get(0)))), epochs);
        boolean stop = false;
        for (Path f : files) {
            long base = baseOf(f);
            if (segs.isEmpty()) nextSeq = base;
            if (stop || (base > maxSeq && !segs.isEmpty())) {   // past the cut
                Files.deleteIfExists(f);
                Files.deleteIfExists(snapFile(base));
                continue;
            }
            Segment s = new Segment(base, f, FileChannel.open(f, READ, WRITE));
            segs.add(s);
            buf.clear();
            while (buf.hasRemaining()) if (s.ch.read(buf, buf.position()) <= 0) break;
            int n = buf.position(), p = 0;
            while (p + R_HDR <= n) {
                int len = buf.getInt(p);
                if (len < R_HDR || len > MAX_RECORD || p + len > n) break;
                if (buf.getLong(p + R_SEQ) != nextSeq || nextSeq > maxSeq || !crcOk(p, len)) break;
                s.index(nextSeq, p);
                note(nextSeq, buf.getInt(p + R_EPOCH), buf.getLong(p + R_PRODUCER), buf.getLong(p + R_PSEQ));
                p += len;
                nextSeq++;
            }
            s.size = p;
            buf.limit(buf.capacity()).position(p);
            if (p < n) {
                s.ch.truncate(p);
                if (maxSeq == Long.MAX_VALUE) tornBytes += n - p;
                stop = true;
            }
            active = s;
            flushed = p;
        }
    }

    private static long baseOf(Path f) { return Long.parseLong(f.getFileName().toString().replace(".log", "")); }

    private boolean crcOk(int p, int len) {
        view.limit(p + len).position(p + 8);
        crc.reset();
        crc.update(view);
        return (int) crc.getValue() == buf.getInt(p + R_CRC);
    }

    /**
     * Where a standby's log stops agreeing with this one (Kafka's KIP-101 rule). One epoch has one
     * leader, so the same (epoch, seq) is the same record. Take the newest epoch both logs know and
     * cut at the shorter end of it.
     */
    long commonPrefix(long[] epochOf, long[] startOf, int n, long peerLast) {
        for (int i = n - 1; i >= 0; i--) {
            long peerEnd = i + 1 < n ? startOf[i + 1] - 1 : peerLast;
            for (int j = epochs.size() - 1; j >= 0; j--) {
                if (epochs.get(j)[0] != epochOf[i]) continue;
                long myEnd = j + 1 < epochs.size() ? epochs.get(j + 1)[1] - 1 : lastSeq();
                return Math.min(myEnd, peerEnd);
            }
        }
        return 0;
    }

    @Override public void close() throws IOException {
        flush();
        for (Segment s : segs) s.ch.close();
    }
}
