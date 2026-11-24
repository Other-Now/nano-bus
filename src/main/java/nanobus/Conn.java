package nanobus;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

/** One socket on a shard. Only the shard's thread touches it, except the volatile metric fields. */
final class Conn {
    static final int NEW = 0, PUB = 1, SUB = 2, REPLICA = 3 /* leader -> standby */, UPSTREAM = 4 /* standby -> leader */;
    private static final int ACK_RING = 1024;

    final long id;
    final SocketChannel ch;
    final ByteBuffer in;
    final ByteBuffer out;
    SelectionKey key;
    int kind = NEW;
    boolean closed, closeAfterFlush;

    // PUB: a ring of (journal seq, producer seq) pairs waiting for the commit index
    ProducerTable.State producer;
    boolean gotPublish;
    private final long[] ackLog = new long[ACK_RING], ackPseq = new long[ACK_RING];
    private int ackHead, ackTail;

    // SUB / REPLICA
    Journal.Cursor cursor;
    byte[] topic;                // null = whole shard
    long ackSeq, lastAckNs;      // REPLICA

    long lastReadNs = System.nanoTime();
    volatile long mNextSeq;      // metrics: the subscriber's position

    Conn(long id, SocketChannel ch, ByteBuffer in, ByteBuffer out) {
        this.id = id;
        this.ch = ch;
        this.in = in;
        this.out = out;
    }

    void pushAck(long logSeq, long pseq) {
        int i;
        if (ackTail - ackHead == ACK_RING) i = (ackTail - 1) & (ACK_RING - 1);   // full: merge into the newest
        else i = ackTail++ & (ACK_RING - 1);
        ackLog[i] = logSeq;
        ackPseq[i] = pseq;
    }

    /** Highest producer seq whose record is committed, or -1 if nothing new. */
    long popAck(long commit) {
        long r = -1;
        while (ackHead != ackTail && ackLog[ackHead & (ACK_RING - 1)] <= commit) r = ackPseq[ackHead++ & (ACK_RING - 1)];
        return r;
    }

    /** Writes what the socket takes, and asks for OP_WRITE only while something is left. */
    void flush() throws IOException {
        if (out.position() > 0) {
            out.flip();
            ch.write(out);
            if (out.hasRemaining()) out.compact(); else out.clear();
        }
        int want = out.position() > 0 ? SelectionKey.OP_READ | SelectionKey.OP_WRITE : SelectionKey.OP_READ;
        if (key.isValid() && key.interestOps() != want) key.interestOps(want);
    }
}
