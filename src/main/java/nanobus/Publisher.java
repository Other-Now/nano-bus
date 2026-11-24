package nanobus;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static nanobus.Proto.*;

/**
 * Publishes with a producer id and a dense sequence per shard. Frames stay in a window until the broker
 * says they are committed; after a reconnect the whole window is sent again and the broker drops what it
 * already has. Not thread-safe: call offer() and poll() from one thread.
 */
public final class Publisher implements AutoCloseable {
    private final PubLink[] links;

    public Publisher(String brokers, int shards, long producerId) {
        links = new PubLink[shards];
        for (int i = 0; i < shards; i++) links[i] = new PubLink(brokers, i, producerId);
    }

    /** Queues a message. Returns its sequence, or 0 if that shard's window is full (back-pressure). */
    public long offer(int shard, byte[] topic, byte[] payload, int payloadLen, long ts) {
        return links[shard].offer(topic, payload, payloadLen, ts);
    }

    /** Moves bytes in both directions; call it often. Returns true if it did anything. */
    public boolean poll() {
        boolean w = false;
        for (PubLink l : links) w |= l.poll();
        return w;
    }

    public boolean ready() {
        for (PubLink l : links) if (!l.ready()) return false;
        return true;
    }

    public long acked(int shard) { return links[shard].acked; }
    public long offered(int shard) { return links[shard].nextSeq - 1; }
    public long resent() { long n = 0; for (PubLink l : links) n += l.resent; return n; }
    public long reconnects() { long n = 0; for (PubLink l : links) n += l.reconnects; return n; }

    @Override public void close() { for (PubLink l : links) l.close(); }

    private static final class PubLink extends Link {
        private static final int WINDOW = 8 << 20;
        private final long producerId;
        private final ByteBuffer win = ByteBuffer.allocateDirect(WINDOW);   // position = end of queued frames
        private final ByteBuffer send = win.duplicate();
        private int head;        // first byte not yet acked
        private int sent;        // first byte not yet written to this connection
        private int maxSent;     // furthest byte ever written (so resends can be counted)
        long nextSeq = 1, acked, resent;

        PubLink(String brokers, int shard, long producerId) {
            super(brokers, shard);
            this.producerId = producerId;
        }

        long offer(byte[] topic, byte[] payload, int payloadLen, long ts) {
            int body = PUB_HDR + topic.length + payloadLen;
            if (win.remaining() < FRAME_HDR + body) {
                if (head < WINDOW / 2) return 0;
                win.limit(win.position()).position(head).compact();     // at most half the window moves
                sent -= head;
                maxSent -= head;
                head = 0;
            }
            frame(win, PUBLISH, body);
            win.putLong(nextSeq).putLong(ts).putShort((short) topic.length).put(topic).put(payload, 0, payloadLen);
            return nextSeq++;
        }

        @Override void hello(ByteBuffer out) {
            frame(out, HELLO_PUB, 8);
            out.putLong(producerId);
        }

        @Override void onReady(long lastInBroker) {
            // lastInBroker may be appended but not committed, so it is not an ack: resend the whole window.
            if (nextSeq == 1 && lastInBroker > 0) { nextSeq = lastInBroker + 1; acked = lastInBroker; }
            sent = head;
        }

        @Override void onFrame(byte type, ByteBuffer b, int p, int len) {
            if (type != PUB_ACK) return;
            long a = b.getLong(p);
            while (head < win.position() && win.getLong(head + FRAME_HDR) <= a) head += FRAME_HDR + win.getInt(head);
            if (sent < head) sent = head;
            if (a > acked) acked = a;
            if (head == win.position()) { win.clear(); head = sent = maxSent = 0; }
        }

        @Override int writeTo(SocketChannel ch) throws IOException {
            int end = win.position();
            if (sent >= end) return 0;
            send.limit(end).position(sent);
            int n = ch.write(send);
            if (sent < maxSent) resent += Math.min(n, maxSent - sent);
            sent += n;
            if (sent > maxSent) maxSent = sent;
            return n;
        }
    }
}
