package nanobus;

import java.nio.file.Path;

public final class BrokerConfig {
    public String id = "A";
    public String host = "127.0.0.1";
    public int port = 7100;                 // shard i listens on port + i
    public int shards = 2;
    public String peerId = "B";
    public String peerHost = "127.0.0.1";
    public int peerPort;                    // 0 = no standby
    public String witnessHost = "127.0.0.1";
    public int witnessPort = 7000;
    public int metricsPort;                 // 0 = off
    public Path dir = Path.of("work/A");
    public int segMb = 16;
    public int maxSegments = 16;
    public boolean fsync;                   // force() once per event-loop batch
    public boolean ackAll = true;           // false = ack before the standby has it (the unsafe control)
    public boolean dedup = true;            // false = no producer sequence check (the unsafe control)
    public boolean poolBuffers = true;      // false = allocateDirect per connection (see docs/NOTES.md)
    public int replTimeoutMs = 400;
    public int heapCache;                   // GC experiment: keep the last N records per shard as heap objects
    public boolean plantLeak;               // leak exercise: forget to unregister closed subscribers

    public static BrokerConfig parse(Args a) {
        BrokerConfig c = new BrokerConfig();
        c.id = a.str("id", c.id);
        c.peerId = a.str("peer-id", c.id.equals("A") ? "B" : "A");
        c.host = a.str("host", c.host);
        c.port = a.num("port", c.port);
        c.shards = a.num("shards", c.shards);
        String peer = a.str("peer", "");
        if (!peer.isEmpty()) {
            int i = peer.indexOf(":");
            c.peerHost = peer.substring(0, i);
            c.peerPort = Integer.parseInt(peer.substring(i + 1));
        }
        String w = a.str("witness", c.witnessHost + ":" + c.witnessPort);
        int i = w.indexOf(":");
        c.witnessHost = w.substring(0, i);
        c.witnessPort = Integer.parseInt(w.substring(i + 1));
        c.metricsPort = a.num("metrics", 0);
        c.dir = Path.of(a.str("dir", "work/" + c.id));
        c.segMb = a.num("seg-mb", c.segMb);
        c.maxSegments = a.num("segments", c.maxSegments);
        c.fsync = a.flag("fsync");
        c.ackAll = !a.str("ack", "all").equals("leader");
        c.dedup = !a.str("dedup", "on").equals("off");
        c.poolBuffers = !a.str("pool", "on").equals("off");
        c.replTimeoutMs = a.num("repl-timeout", c.replTimeoutMs);
        c.heapCache = a.num("heap-cache", 0);
        c.plantLeak = a.flag("plant-leak");
        return c;
    }
}
