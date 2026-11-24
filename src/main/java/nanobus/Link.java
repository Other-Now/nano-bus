package nanobus;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static nanobus.Proto.*;

/**
 * One client connection to whichever broker currently leads a shard. Non-blocking and driven by poll(),
 * so a client can run on the caller's own thread. Reconnects by alternating between the brokers.
 */
abstract class Link {
    private static final long SILENCE_NS = 500_000_000L;     // brokers send a heartbeat every 100 ms
    private static final long RETRY_NS = 10_000_000L;

    private final InetSocketAddress[] brokers;
    private int which;
    private SocketChannel ch;
    private boolean helloSent;
    private boolean ready;                                   // the broker answered HELLO_OK
    private long lastReadNs, retryNs = System.nanoTime();
    protected final ByteBuffer in = ByteBuffer.allocateDirect(BUF);
    protected final ByteBuffer out = ByteBuffer.allocateDirect(256);
    int epoch;
    long reconnects;

    /** brokers: "host:port,host:port"; ports are the base ports. */
    Link(String brokers, int shard) {
        String[] p = brokers.split(",");
        this.brokers = new InetSocketAddress[p.length];
        for (int i = 0; i < p.length; i++) {
            int c = p[i].indexOf(":");
            this.brokers[i] = new InetSocketAddress(p[i].substring(0, c), Integer.parseInt(p[i].substring(c + 1)) + shard);
        }
    }

    abstract void hello(ByteBuffer out);
    abstract void onReady(long value);
    abstract void onFrame(byte type, ByteBuffer b, int p, int len);
    /** Called when ready: write whatever is waiting. Returns bytes written. */
    abstract int writeTo(SocketChannel ch) throws IOException;

    boolean ready() { return ready; }

    /** Returns true if it moved any bytes. */
    boolean poll() {
        long now = System.nanoTime();
        try {
            if (ch == null) {
                if (now - retryNs < 0) return false;
                ch = SocketChannel.open();
                ch.configureBlocking(false);
                ch.setOption(StandardSocketOptions.TCP_NODELAY, true);
                lastReadNs = now;
                in.clear();
                out.clear();
                helloSent = false;
                ch.connect(brokers[which]);
            }
            if (!ch.isConnected()) {
                if (!ch.finishConnect()) {
                    if (now - lastReadNs > SILENCE_NS) drop(now);
                    return false;
                }
            }
            if (!helloSent) { helloSent = true; hello(out); }
            int moved = 0;
            if (out.position() > 0) {
                out.flip();
                moved += ch.write(out);
                out.compact();
            }
            if (ready) moved += writeTo(ch);
            int n = ch.read(in);
            if (n < 0) { drop(now); return true; }
            if (n > 0) {
                lastReadNs = now;
                moved += n;
                frames(now);
            } else if (now - lastReadNs > SILENCE_NS) {
                drop(now);
            }
            return moved > 0;
        } catch (IOException e) {
            drop(now);
            return false;
        }
    }

    private void frames(long now) {
        int p = 0, end = in.position();
        while (end - p >= FRAME_HDR) {
            int len = in.getInt(p);
            if (end - p - FRAME_HDR < len) break;
            byte type = in.get(p + 4);
            int body = p + FRAME_HDR;
            if (type == NOT_LEADER) { drop(now); return; }
            if (type == HELLO_OK) {
                epoch = in.getInt(body);
                ready = true;
                onReady(in.getLong(body + 4));
            } else if (type != HEARTBEAT) {
                onFrame(type, in, body, len);
            }
            p = body + len;
        }
        if (p > 0) {
            if (p == end) in.clear();
            else in.position(p).limit(end).compact();
        }
    }

    private void drop(long now) {
        close();
        which = (which + 1) % brokers.length;
        retryNs = now + RETRY_NS;
        reconnects++;
    }

    void close() {
        ready = false;
        if (ch != null) {
            try { ch.close(); } catch (IOException ignored) { /* nothing to do */ }
            ch = null;
        }
    }
}
