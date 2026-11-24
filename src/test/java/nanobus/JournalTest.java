package nanobus;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static nanobus.Proto.*;
import static org.junit.jupiter.api.Assertions.*;

class JournalTest {
    @TempDir Path dir;
    private static final int SEG = 128 * 1024;

    private Journal open() throws IOException { return new Journal(dir, SEG, 1000, false); }

    private static long append(Journal j, int epoch, long producer, long pseq, String topic, int payload) throws IOException {
        byte[] t = topic.getBytes();
        ByteBuffer b = ByteBuffer.allocateDirect(t.length + payload);
        b.put(t);
        for (int i = 0; i < payload; i++) b.put((byte) (pseq + i));
        return j.append(epoch, producer, pseq, 42, b, 0, t.length, t.length + payload);
    }

    /** "seq:producer:pseq" for every record from a seq on. */
    private static List<String> read(Journal j, long from, long limit, byte[] topic) throws IOException {
        List<String> out = new ArrayList<>();
        Journal.Cursor c = new Journal.Cursor(from);
        ByteBuffer d = ByteBuffer.allocateDirect(MAX_RECORD * 2);
        while (c.seq <= limit) {
            d.clear();
            j.read(c, limit, d, d.capacity(), topic);
            d.flip();
            for (int p = 0; p < d.limit(); p += d.getInt(p)) {
                out.add(d.getLong(p + R_SEQ) + ":" + d.getLong(p + R_PRODUCER) + ":" + d.getLong(p + R_PSEQ));
            }
        }
        return out;
    }

    @Test void readsBackAcrossSegmentsAndReopen() throws IOException {
        List<String> want = new ArrayList<>();
        try (Journal j = open()) {
            for (int i = 1; i <= 3000; i++) want.add(append(j, 1, 7, i, "t", 200) + ":7:" + i);
            assertTrue(j.segments() > 3);
            assertEquals(want, read(j, 1, 3000, null));
            assertEquals(want.subList(1233, 1500), read(j, 1234, 1500, null));
        }
        try (Journal j = open()) {
            assertEquals(3000, j.lastSeq());
            assertEquals(want, read(j, 1, 3000, null));
            assertEquals(3000, j.producers.get(7).lastPseq);
            assertEquals(3001, append(j, 1, 7, 3001, "t", 10));
        }
    }

    @Test void topicFilter() throws IOException {
        try (Journal j = open()) {
            for (int i = 1; i <= 2000; i++) append(j, 1, 1, i, i % 10 == 0 ? "aa" : "ab", 200);
            List<String> got = read(j, 1, 2000, "aa".getBytes());
            assertEquals(200, got.size());
            assertEquals("10:1:10", got.get(0));
            assertEquals("2000:1:2000", got.get(199));
        }
    }

    @Test void tornTailIsDropped() throws IOException {
        try (Journal j = open()) { for (int i = 1; i <= 10; i++) append(j, 1, 1, i, "t", 100); }
        Path f = lastLog();
        try (FileChannel c = FileChannel.open(f, StandardOpenOption.WRITE)) { c.truncate(Files.size(f) - 30); }
        try (Journal j = open()) {
            assertEquals(9, j.lastSeq());
            assertTrue(j.tornBytes > 0);
            assertEquals(9, j.producers.get(1).lastPseq);
            append(j, 1, 1, 10, "t", 50);
        }
        try (Journal j = open()) {
            assertEquals(10, j.lastSeq());
            assertEquals(0, j.tornBytes);
        }
    }

    @Test void flippedBitStopsRecovery() throws IOException {
        try (Journal j = open()) { for (int i = 1; i <= 10; i++) append(j, 1, 1, i, "t", 100); }
        try (FileChannel c = FileChannel.open(lastLog(), StandardOpenOption.WRITE)) {
            c.write(ByteBuffer.wrap(new byte[]{99}), 5 * (R_HDR + 101) + R_HDR + 20);      // inside record 6
        }
        try (Journal j = open()) { assertEquals(5, j.lastSeq()); }
    }

    @Test void truncateRebuildsProducersAndEpochs() throws IOException {
        try (Journal j = open()) {
            for (int i = 1; i <= 2000; i++) append(j, i <= 1500 ? 1 : 4, i % 2, (i + 1) / 2, "t", 200);
            j.truncateAfter(1400);
            assertEquals(1400, j.lastSeq());
            assertEquals(700, j.producers.get(0).lastPseq);
            assertEquals(1, j.epochs.size());
            assertEquals(1401, append(j, 5, 1, 701, "t", 200));
            assertEquals(1401, read(j, 1, 1401, null).size());
            assertEquals(5, j.epochs.get(1)[0]);
        }
    }

    @Test void retentionKeepsProducerState() throws IOException {
        try (Journal j = new Journal(dir, SEG, 3, false)) {
            append(j, 1, 99, 1, "t", 10);
            for (int i = 1; i <= 5000; i++) append(j, 2, 1, i, "t", 200);
            assertTrue(j.firstSeq() > 2000);
        }
        try (Journal j = new Journal(dir, SEG, 3, false)) {
            assertEquals(1, j.producers.get(99).lastPseq, "producer 99 wrote only in a deleted segment");
            assertEquals(2, j.epochs.size());
            assertEquals(5001, j.lastSeq());
        }
    }

    @Test void cursorSurvivesRetentionOfTheSegmentItJustFinished() throws IOException {
        try (Journal j = new Journal(dir, SEG, 2, false)) {
            Journal.Cursor c = new Journal.Cursor(1);
            ByteBuffer d = ByteBuffer.allocateDirect(MAX_RECORD * 2);
            int i = 1;
            while (j.segments() < 2) append(j, 1, 1, i++, "t", 200);
            long endOfFirst = j.lastSeq() - 1;
            while (c.seq <= endOfFirst) j.read(c, endOfFirst, d.clear(), d.capacity(), null);   // parked on the boundary
            while (j.firstSeq() <= endOfFirst) append(j, 1, 1, i++, "t", 200);
            assertEquals(c.seq, j.firstSeq());
            d.clear();
            assertTrue(j.read(c, j.lastSeq(), d, d.capacity(), null) > 0);
            assertEquals(endOfFirst + 1, d.getLong(R_SEQ));
        }
    }

    @Test void commonPrefixOfDivergedLogs() throws IOException {
        Path d2 = dir.resolveSibling(dir.getFileName() + "-b");
        try (Journal leader = open(); Journal old = new Journal(d2, SEG, 100, false)) {
            for (int i = 1; i <= 100; i++) { append(leader, 1, 1, i, "t", 10); append(old, 1, 1, i, "t", 10); }
            for (int i = 101; i <= 130; i++) append(old, 1, 1, i, "t", 10);          // never replicated
            for (int i = 101; i <= 150; i++) append(leader, 2, 1, i, "t", 10);       // the new leader's records
            assertEquals(100, prefix(leader, old));
            for (int i = 131; i <= 140; i++) append(old, 3, 1, i, "t", 10);          // an epoch the leader never saw
            assertEquals(100, prefix(leader, old));
            old.truncateAfter(100);
            assertEquals(100, prefix(leader, old));
        }
    }

    private static long prefix(Journal leader, Journal peer) {
        int n = peer.epochs.size();
        long[] e = new long[n], s = new long[n];
        for (int i = 0; i < n; i++) { e[i] = peer.epochs.get(i)[0]; s[i] = peer.epochs.get(i)[1]; }
        return leader.commonPrefix(e, s, n, peer.lastSeq());
    }

    @Test void randomOperationsMatchAModel() throws IOException {
        for (int seed = 0; seed < 20; seed++) {
            Random r = new Random(seed);
            Path d = dir.resolve("s" + seed);
            List<String> model = new ArrayList<>();
            long[] pseq = new long[4];
            Journal j = new Journal(d, SEG, 1000, false);
            int epoch = 1;
            for (int op = 0; op < 400; op++) {
                int x = r.nextInt(100);
                if (x < 80) {
                    for (int k = r.nextInt(30); k >= 0; k--) {
                        int p = r.nextInt(4);
                        long seq = append(j, epoch, p, ++pseq[p], "t" + r.nextInt(3), r.nextInt(500));
                        model.add(seq + ":" + p + ":" + pseq[p]);
                    }
                    if (r.nextBoolean()) j.flush();
                } else if (x < 87) {
                    j.close();
                    j = new Journal(d, SEG, 1000, false);
                } else if (x < 93 && !model.isEmpty()) {
                    int keep = model.size() - r.nextInt(Math.min(model.size(), 200) + 1);
                    j.truncateAfter(keep);
                    model = new ArrayList<>(model.subList(0, keep));
                    for (int p = 0; p < 4; p++) pseq[p] = 0;
                    for (String m : model) { String[] f = m.split(":"); pseq[Integer.parseInt(f[1])] = Long.parseLong(f[2]); }
                    epoch++;
                } else if (!model.isEmpty()) {
                    long from = 1 + r.nextInt(model.size());
                    assertEquals(model.subList((int) from - 1, model.size()), read(j, from, model.size(), null));
                }
                assertEquals(model.size(), j.lastSeq());
            }
            for (int p = 0; p < 4; p++) assertEquals(pseq[p], j.producers.get(p).lastPseq);
            j.close();
        }
    }

    private Path lastLog() throws IOException {
        try (var st = Files.list(dir)) { return st.filter(p -> p.toString().endsWith(".log")).sorted().reduce((a, b) -> b).orElseThrow(); }
    }
}
