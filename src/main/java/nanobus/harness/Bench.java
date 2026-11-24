package nanobus.harness;

import nanobus.Args;
import nanobus.Publisher;
import nanobus.Subscriber;
import org.HdrHistogram.Histogram;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Open-loop latency: publish -> leader -> standby -> commit -> subscriber. The timestamp is the time the
 * message was DUE, not when it was really sent, so a stall is charged to every message it delayed.
 * Publisher and subscriber share this JVM, so both ends read one clock.
 */
public final class Bench {
    private static final Map<String, List<String>> GC = Map.of(
            "G1", List.of("-XX:+UseG1GC"),
            "G1-inline-housekeeping", List.of("-XX:+UseG1GC", "-Dnanobus.inlineHousekeeping=true"),
            "Parallel", List.of("-XX:+UseParallelGC"),
            "Shenandoah", List.of("-XX:+UseShenandoahGC"),
            "ZGC", List.of("-XX:+UseZGC"),
            "GenZGC", List.of("-XX:+UseZGC", "-XX:+ZGenerational"));
    /** ZGC stops an allocating thread (not the world) when it cannot free memory fast enough. */
    private static final Pattern STALL = Pattern.compile("Allocation Stall .* ([0-9.]+)ms");
    private static final Pattern PAUSE = Pattern.compile("Pause.* ([0-9.]+)ms");

    public static int run(Args a) throws Exception {
        String[] rates = a.str("rates", "50000,100000").split(",");
        String[] gcs = a.str("gcs", "G1").split(",");
        int seconds = a.num("seconds", 20), warm = a.num("warmup", 8), payload = a.num("payload", 128);
        int shards = a.num("shards", 1), cache = a.num("heap-cache", 0), reps = a.num("reps", 1);
        boolean solo = a.flag("solo"), spin = a.flag("spin"), fsync = a.flag("fsync");
        String heap = a.str("heap", "256m"), tag = a.str("tag", "latency");
        StringBuilder md = new StringBuilder("| GC | rate/s | achieved/s | p50 us | p99 us | p99.9 us | max ms | broker B/msg | GCs | longest GC pause ms | GC pause total ms | alloc stalls (longest ms) |\n|---|---|---|---|---|---|---|---|---|---|---|---|\n");
        String floor = floor();
        System.out.println(floor);
        for (int rep = 0; rep < reps; rep++) {
            for (String gc : gcs) {
                for (String r : rates) {
                    md.append(one(gc, Integer.parseInt(r), seconds, warm, payload, shards, cache, solo, fsync, heap, spin)).append('\n');
                    System.out.println(md.substring(md.lastIndexOf("\n", md.length() - 2) + 1).trim());
                }
            }
        }
        Path out = Path.of(a.str("out", "results/local"));
        Files.createDirectories(out);
        md.append(System.lineSeparator()).append(floor).append(System.lineSeparator());
        Files.writeString(out.resolve(tag + ".md"), md);
        System.out.println(md);
        return 0;
    }

    /** What this machine does to a thread that only reads the clock: no latency below can beat it. */
    private static String floor() throws InterruptedException {
        Thread[] t = new Thread[4];
        Histogram[] h = new Histogram[t.length];
        for (int i = 0; i < t.length; i++) {
            Histogram x = h[i] = new Histogram(60_000_000_000L, 3);
            t[i] = Thread.ofPlatform().start(() -> {
                long due = System.nanoTime(), end = due + 5_000_000_000L;
                for (; due < end; due += 10_000) {          // an event due every 10 us, like the bench schedule
                    long n;
                    while ((n = System.nanoTime()) < due) Thread.onSpinWait();
                    x.recordValue(n - due);
                }
            });
        }
        for (Thread x : t) x.join();
        for (int i = 1; i < t.length; i++) h[0].add(h[i]);
        return String.format(Locale.ROOT, "Machine floor (%d spinning threads, %d CPUs, idle otherwise): p99 %.0f us, p99.9 %.0f us, max %.1f ms", t.length,
                Runtime.getRuntime().availableProcessors(), h[0].getValueAtPercentile(99) / 1e3, h[0].getValueAtPercentile(99.9) / 1e3, h[0].getMaxValue() / 1e6);
    }

    private static String one(String gc, int rate, int seconds, int warm, int payloadLen, int shards, int cache, boolean solo,
                              boolean fsync, String heap, boolean spin) throws Exception {
        Path dir = Path.of("work/bench");
        List<String> jvm = new ArrayList<>(GC.get(gc));
        jvm.addAll(List.of("-Xms" + heap, "-Xmx" + heap, "-XX:+AlwaysPreTouch", "-XX:MaxDirectMemorySize=256m",
                "-Xlog:gc*,safepoint:file=" + dir.resolve("gc-%n.log").toString().replace('\\', '/') + ":uptime:filecount=0"));
        List<String> extra = new ArrayList<>(List.of("--segments", "8"));
        if (cache > 0) extra.addAll(List.of("--heap-cache", "" + cache));
        if (fsync) extra.add("--fsync");
        try (Cluster c = new Cluster(dir, shards, jvm, extra)) {
            c.start(0);
            while (c.leader() != 0) Thread.sleep(50);
            if (!solo) {
                c.start(1);
                if (c.awaitInSync(20_000) < 0) throw new IllegalStateException("no standby, see " + dir);
            }
            byte[] topic = "md.trades".getBytes();
            byte[] payload = new byte[payloadLen];
            Histogram h = new Histogram(60_000_000_000L, 3);
            Publisher pub = new Publisher(Cluster.BROKERS, shards, 1);
            long[] measureFrom = {Long.MAX_VALUE}, received = new long[1];
            boolean[] stop = new boolean[1];
            Subscriber sub = new Subscriber(Cluster.BROKERS, shards, -1, null,
                    (sh, seq, producer, pseq, ts, b, to, tl, po, pl) -> {
                        if (ts >= measureFrom[0]) h.recordValue(Math.min(60_000_000_000L, Math.max(0, System.nanoTime() - ts)));
                        received[0]++;
                    });
            while (!sub.ready() || !pub.ready()) { sub.poll(); pub.poll(); }
            Thread st = Thread.ofPlatform().name("sub").start(() -> { while (!stopped(stop)) if (!sub.poll()) idle(spin); });

            long start = System.nanoTime() + 1_000_000, from = start + warm * 1_000_000_000L, end = from + seconds * 1_000_000_000L;
            measureFrom[0] = from;
            long n = 0;
            double gap = 1e9 / rate;
            long gc0 = selfGcs();
            // Scraped from its own thread: an HTTP call on the publishing thread stalls the schedule.
            AtomicReference<double[]> first = new AtomicReference<>();
            Thread scraper = Thread.ofPlatform().start(() -> {
                long wait = from - System.nanoTime();
                LockSupport.parkNanos(wait);
                first.set(broker(c));
            });
            while (true) {
                long now = System.nanoTime();
                if (now >= end) break;
                for (int k = 0; k < 128; k++) {
                    long due = start + (long) (n * gap);
                    if (due > now) break;
                    if (pub.offer((int) (n % shards), topic, payload, payloadLen, due) == 0) break;
                    n++;
                }
                if (!pub.poll()) idle(spin);
            }
            long lag = n - total(pub, shards);
            double[] b1 = broker(c);
            scraper.join();
            double[] b0 = first.get();
            for (long until = System.nanoTime() + 10_000_000_000L; total(pub, shards) < n && System.nanoTime() < until; ) pub.poll();
            Thread.sleep(300);
            synchronized (stop) { stop[0] = true; }
            st.join();
            long gcs = selfGcs() - gc0;
            long expect = (long) (rate * (double) seconds);
            double msgs = b1[0] - b0[0];
            double[] p = pauses(dir.resolve("gc-A.log"), warm);
            pub.close();
            sub.close();
            return String.format(Locale.ROOT, "| %s | %d | %.0f%s | %.0f | %.0f | %.0f | %.1f | %.2f | %.0f | %.2f | %.1f | %.0f (%.1f) |", gc, rate,
                    h.getTotalCount() / (double) seconds, h.getTotalCount() < expect * 0.99 || lag > rate / 10 ? " (saturated)" : "",
                    h.getValueAtPercentile(50) / 1e3, h.getValueAtPercentile(99) / 1e3, h.getValueAtPercentile(99.9) / 1e3,
                    h.getMaxValue() / 1e6, (b1[1] - b0[1]) / msgs, b1[2] - b0[2], p[0], p[1], p[2], p[3])
                    + (gcs > 0 ? " (load generator had " + gcs + " GCs)" : "");
        }
    }

    private static void idle(boolean spin) { if (spin) Thread.onSpinWait(); else LockSupport.parkNanos(20_000); }

    private static boolean stopped(boolean[] s) { synchronized (s) { return s[0]; } }

    private static long total(Publisher p, int shards) {
        long t = 0;
        for (int i = 0; i < shards; i++) t += p.acked(i);
        return t;
    }

    private static double[] broker(Cluster c) {
        return new double[]{c.metric(0, "nanobus_published_total"), c.metric(0, "nanobus_thread_allocated_bytes_total"),
                c.metric(0, "jvm_gc_collections_total")};
    }

    private static long selfGcs() {
        long n = 0;
        for (GarbageCollectorMXBean b : ManagementFactory.getGarbageCollectorMXBeans()) n += b.getCollectionCount();
        return n;
    }

    /** Longest and total stop-the-world GC pause, after warm-up, from the unified GC log. */
    private static double[] pauses(Path log, int warm) throws Exception {
        double max = 0, sum = 0, stalls = 0, longest = 0;
        for (String l : Files.readAllLines(log)) {
            Matcher a = STALL.matcher(l);
            if (a.find() && Double.parseDouble(l.substring(1, l.indexOf('s'))) >= warm) {
                stalls++;
                longest = Math.max(longest, Double.parseDouble(a.group(1)));
            }
            Matcher m = PAUSE.matcher(l);
            if (l.contains("[safepoint") || l.contains("gc,start") || !m.find()) continue;
            double up = Double.parseDouble(l.substring(1, l.indexOf('s')));
            if (up < warm) continue;
            double ms = Double.parseDouble(m.group(1));
            max = Math.max(max, ms);
            sum += ms;
        }
        return new double[]{max, sum, stalls, longest};
    }
}
