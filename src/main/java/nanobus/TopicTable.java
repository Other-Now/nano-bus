package nanobus;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Per-topic message counts, looked up by the topic's bytes in the socket buffer so the hot path never
 * builds a String. One writer (the shard); the metrics thread reads counts and works out rates.
 */
final class TopicTable {
    static final class Stat {
        final byte[] name;
        final String text;
        private long n;
        volatile long count;
        Stat(byte[] name) { this.name = name; this.text = new String(name, StandardCharsets.UTF_8); }
    }

    private static final int SLOTS = 4096, MAX = 2048;     // past MAX topics, new ones are not tracked
    final Stat[] slots = new Stat[SLOTS];
    private int size;

    void hit(ByteBuffer b, int p, int len) {
        int h = 0x811c9dc5;
        for (int i = 0; i < len; i++) h = (h ^ b.get(p + i)) * 0x01000193;
        for (int i = (h ^ h >>> 15) & (SLOTS - 1); ; i = (i + 1) & (SLOTS - 1)) {
            Stat s = slots[i];
            if (s == null) {
                if (size == MAX) return;
                byte[] name = new byte[len];
                b.get(p, name);
                s = new Stat(name);
                slots[i] = s;
                size++;
            } else if (!same(s.name, b, p, len)) {
                continue;
            }
            s.count = ++s.n;
            return;
        }
    }

    private static boolean same(byte[] a, ByteBuffer b, int p, int len) {
        if (a.length != len) return false;
        for (int i = 0; i < len; i++) if (a[i] != b.get(p + i)) return false;
        return true;
    }
}
