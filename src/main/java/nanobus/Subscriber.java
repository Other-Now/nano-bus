package nanobus;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static nanobus.Proto.*;

/**
 * Reads one or more shards from a position. After a reconnect it resumes at the next sequence it wants,
 * so the stream it hands out has no repeats. Not thread-safe: call poll() from one thread.
 */
public final class Subscriber implements AutoCloseable {
    /** The record is a view into the socket buffer and is only valid during the call. */
    public interface Handler {
        void onMessage(int shard, long seq, long producer, long pseq, long ts, ByteBuffer buf, int topicOff, int topicLen, int payloadOff, int payloadLen);
        default void onGap(int shard, long from, long to) {}
    }

    private final SubLink[] links;

    /** fromSeq: 1 = everything retained, -1 = only new messages. topic null = the whole shard. */
    public Subscriber(String brokers, int shards, long fromSeq, byte[] topic, Handler h) {
        links = new SubLink[shards];
        for (int i = 0; i < shards; i++) links[i] = new SubLink(brokers, i, fromSeq, topic, h);
    }

    public boolean poll() {
        boolean w = false;
        for (SubLink l : links) w |= l.poll();
        return w;
    }

    public boolean ready() {
        for (SubLink l : links) if (!l.ready()) return false;
        return true;
    }

    public long nextSeq(int shard) { return links[shard].next; }
    public long reconnects() { long n = 0; for (SubLink l : links) n += l.reconnects; return n; }

    @Override public void close() { for (SubLink l : links) l.close(); }

    private static final class SubLink extends Link {
        private final int shard;
        private final byte[] topic;
        private final Handler h;
        long next;

        SubLink(String brokers, int shard, long from, byte[] topic, Handler h) {
            super(brokers, shard);
            this.shard = shard;
            this.next = from;
            this.topic = topic;
            this.h = h;
        }

        @Override void hello(ByteBuffer out) {
            int tl = topic == null ? 0 : topic.length;
            frame(out, SUBSCRIBE, 10 + tl);
            out.putLong(next).putShort((short) tl);
            if (tl > 0) out.put(topic);
        }

        @Override void onReady(long cursor) { if (next < 0) next = cursor; }

        @Override void onFrame(byte type, ByteBuffer b, int p, int len) {
            if (type == GAP) {
                long first = b.getLong(p);
                if (first > next) { h.onGap(shard, next, first); next = first; }
                return;
            }
            if (type != DATA) return;
            for (int q = p, end = p + len; q < end; ) {
                int rl = b.getInt(q);
                long seq = b.getLong(q + R_SEQ);
                if (seq >= next) {
                    next = seq + 1;
                    int tl = b.getShort(q + R_TOPIC_LEN);
                    h.onMessage(shard, seq, b.getLong(q + R_PRODUCER), b.getLong(q + R_PSEQ), b.getLong(q + R_TS), b,
                            q + R_HDR, tl, q + R_HDR + tl, rl - R_HDR - tl);
                }
                q += rl;
            }
        }

        @Override int writeTo(SocketChannel ch) { return 0; }
    }
}
