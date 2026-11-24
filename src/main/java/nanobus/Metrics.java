package nanobus;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

/** Prometheus text endpoint on the JDK's built-in HTTP server. Reads volatiles; never touches a shard. */
final class Metrics implements AutoCloseable {
    private final Broker broker;
    private final Thread[] threads;
    private final HttpServer http;
    private final com.sun.management.ThreadMXBean tmx =
            (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();

    Metrics(Broker broker, Thread[] threads) throws IOException {
        this.broker = broker;
        this.threads = threads;
        http = HttpServer.create(new InetSocketAddress("0.0.0.0", broker.cfg.metricsPort), 0);
        http.createContext("/metrics", ex -> {
            byte[] body = render().getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4");
            ex.sendResponseHeaders(200, body.length);
            try (var os = ex.getResponseBody()) { os.write(body); }
        });
        http.start();
    }

    private String render() {
        StringBuilder sb = new StringBuilder(8192);
        boolean leader = broker.isLeader();
        g(sb, "nanobus_leader", leader ? 1 : 0);
        g(sb, "nanobus_epoch", broker.role.epoch());
        g(sb, "nanobus_standby_in_sync", broker.standbyInSet ? 1 : 0);
        g(sb, "nanobus_takeovers_total", broker.takeovers);
        for (Shard s : broker.shards) {
            String l = "{shard=\"" + s.index + "\"}";
            g(sb, "nanobus_published_total" + l, s.mPublished);
            g(sb, "nanobus_duplicates_dropped_total" + l, s.mDups);
            g(sb, "nanobus_producer_gaps_total" + l, s.mPubGaps);
            g(sb, "nanobus_truncated_committed_total" + l, s.mTruncatedCommitted);
            g(sb, "nanobus_delivered_bytes_total" + l, s.mDelivered);
            g(sb, "nanobus_received_bytes_total" + l, s.mBytesIn);
            g(sb, "nanobus_commit_seq" + l, s.mCommit);
            g(sb, "nanobus_uncommitted" + l, s.mLast - s.mCommit);
            g(sb, "nanobus_loop_turns_total" + l, s.mLoops);
            g(sb, "nanobus_thread_allocated_bytes_total" + l, tmx.getThreadAllocatedBytes(threads[s.index].threadId()));
            int pubs = 0, subs = 0;
            for (Conn c : s.conns) {
                if (c.kind == Conn.PUB) pubs++;
                if (c.kind != Conn.SUB) continue;
                subs++;
                if (leader) {
                    g(sb, "nanobus_consumer_lag{shard=\"" + s.index + "\",conn=\"" + c.id + "\"}",
                            Math.max(0, s.mCommit + 1 - c.mNextSeq));
                }
            }
            g(sb, "nanobus_publishers" + l, pubs);
            g(sb, "nanobus_subscribers" + l, subs);
            if (leader) {
                g(sb, "nanobus_replica_lag" + l, s.mReplicaUp ? s.mLast - s.mReplicaAck : -1);
                for (TopicTable.Stat t : s.topics().slots) {
                    if (t != null) g(sb, "nanobus_topic_published_total{topic=\"" + t.text + "\"}", t.count);
                }
            }
        }
        MemoryUsage heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        g(sb, "jvm_heap_used_bytes", heap.getUsed());
        g(sb, "jvm_heap_max_bytes", heap.getMax());
        for (BufferPoolMXBean b : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
            if (b.getName().equals("direct")) {
                g(sb, "jvm_direct_used_bytes", b.getMemoryUsed());
                g(sb, "jvm_direct_buffers", b.getCount());
            }
        }
        for (GarbageCollectorMXBean b : ManagementFactory.getGarbageCollectorMXBeans()) {
            String l = "{gc=\"" + b.getName() + "\"}";
            g(sb, "jvm_gc_collections_total" + l, b.getCollectionCount());
            g(sb, "jvm_gc_millis_total" + l, b.getCollectionTime());
        }
        return sb.toString();
    }

    private static void g(StringBuilder sb, String name, long v) {
        sb.append(name).append(' ').append(String.format(Locale.ROOT, "%d", v)).append('\n');
    }

    @Override public void close() { http.stop(0); }
}
