package nanobus;

import java.nio.ByteBuffer;

/**
 * Wire frames and the journal record layout.
 *
 * Frame:  [int bodyLen][byte type][body...]      (bodyLen excludes these 5 bytes)
 * DATA and REPL_DATA bodies carry journal records verbatim, so fan-out and replication are
 * byte copies: nothing is re-encoded per subscriber.
 */
public final class Proto {
    private Proto() {}

    static final int FRAME_HDR = 5;

    // client -> broker
    static final byte HELLO_PUB = 1;    // long producerId
    static final byte PUBLISH = 2;      // long producerSeq, long ts, short topicLen, topic, payload
    static final byte SUBSCRIBE = 3;    // long fromSeq (-1 = live tail), short topicLen, topic (empty = all)
    // broker -> client
    static final byte HELLO_OK = 10;    // int epoch, long lastProducerSeq
    static final byte PUB_ACK = 11;     // long producerSeq (cumulative)
    static final byte DATA = 12;        // record*
    static final byte GAP = 13;         // long firstAvailableSeq (retention passed the cursor)
    static final byte NOT_LEADER = 14;
    static final byte HEARTBEAT = 15;   // long commitSeq
    // standby -> leader
    static final byte REPL_HELLO = 20;  // int epoch, long firstSeq, long lastSeq, int n, n * (int epoch, long start)
    static final byte REPL_ACK = 21;    // long lastSeq
    // leader -> standby
    static final byte REPL_START = 22;  // int epoch, long truncateTo, int snapLen, snap (snapLen > 0 = reset)
    static final byte REPL_DATA = 23;   // long commitSeq, record*

    // ---- journal record: 48-byte header + topic + payload ----
    static final int R_LEN = 0;         // int, total record bytes
    static final int R_CRC = 4;         // int, CRC32C of bytes [8, len)
    static final int R_SEQ = 8;         // long, dense per shard
    static final int R_EPOCH = 16;      // int, leader epoch that wrote it
    static final int R_TOPIC_LEN = 20;  // short (then 2 bytes of padding)
    static final int R_PRODUCER = 24;   // long
    static final int R_PSEQ = 32;       // long, dense per producer per shard
    static final int R_TS = 40;         // long, opaque to the broker
    static final int R_HDR = 48;

    static final int PUB_HDR = 18;                            // body bytes before the topic in PUBLISH
    static final int MAX_BODY = 32 * 1024;                    // topic + payload
    static final int MAX_RECORD = R_HDR + MAX_BODY;
    static final int BUF = 256 * 1024;                        // per-connection socket buffers
    static final int HEARTBEAT_MS = 100;

    static void frame(ByteBuffer out, byte type, int bodyLen) {
        out.putInt(bodyLen).put(type);
    }

    /** Topics map to shards by hash, so the order of one topic is the order of one shard. */
    public static int shardOf(byte[] topic, int shards) {
        int h = 0x811c9dc5;
        for (byte b : topic) h = (h ^ (b & 0xff)) * 0x01000193;
        h ^= h >>> 16;
        return Math.floorMod(h, shards);
    }
}
