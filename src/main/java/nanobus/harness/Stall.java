package nanobus.harness;

import nanobus.Args;
import nanobus.Publisher;
import nanobus.Subscriber;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * Load for the alert test: one topic publishes for a while and then goes quiet; a second topic keeps
 * going; a subscriber reads for a while and then stops reading without disconnecting.
 */
public final class Stall {
    public static int run(Args a) throws Exception {
        String brokers = a.str("brokers", Cluster.BROKERS);
        int shards = a.num("shards", 2);
        long quietAt = System.nanoTime() + a.num("quiet-after", 40) * 1_000_000_000L;
        long stopReadAt = System.nanoTime() + a.num("stop-reading-after", 40) * 1_000_000_000L;
        long end = System.nanoTime() + a.num("seconds", 120) * 1_000_000_000L;
        AtomicBoolean done = new AtomicBoolean();
        Thread t = Thread.ofPlatform().start(() -> {
            try (Subscriber s = new Subscriber(brokers, shards, -1, null, (sh, q, p, ps, ts, b, to, tl, po, pl) -> {})) {
                while (!done.get()) {
                    if (System.nanoTime() > stopReadAt) { LockSupport.parkNanos(10_000_000); continue; }
                    if (!s.poll()) LockSupport.parkNanos(100_000);
                }
            }
        });
        byte[] quiet = "md.NSE".getBytes(), busy = "md.BSE".getBytes(), payload = new byte[400];
        try (Publisher p = new Publisher(brokers, shards, 7)) {
            while (!p.ready()) p.poll();
            boolean said = false;
            while (System.nanoTime() < end) {
                boolean off = System.nanoTime() > quietAt;
                if (off && !said) { said = true; System.out.println("STALL_AT " + System.currentTimeMillis()); }
                for (int i = 0; i < 10; i++) {
                    if (!off) p.offer(0, quiet, payload, payload.length, 0);
                    p.offer(1 % shards, busy, payload, payload.length, 0);
                }
                p.poll();
                LockSupport.parkNanos(1_000_000);
            }
        }
        done.set(true);
        t.join();
        return 0;
    }
}
