package nanobus.harness;

import nanobus.Args;
import nanobus.Subscriber;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Subscribers that connect, read and leave, over and over - the load that exposes a per-connection leak.
 * Samples the broker's memory once a second. --leak plants the bug, --pool off allocates socket buffers
 * per connection.
 */
public final class Churn {
    public static int run(Args a) throws Exception {
        int seconds = a.num("seconds", 60), perSecond = a.num("per-second", 20);
        String direct = a.str("direct", "256m"), tag = a.str("tag", "churn");
        List<String> jvm = new ArrayList<>(List.of("-Xmx64m", "-XX:MaxDirectMemorySize=" + direct, "-XX:NativeMemoryTracking=summary",
                "-Xlog:gc:file=" + "work/churn/gc.log:uptime:filecount=0"));
        if (a.flag("jfr")) jvm.add("-XX:StartFlightRecording=filename=work/churn/leak.jfr,settings=profile,path-to-gc-roots=true,dumponexit=true");
        List<String> extra = new ArrayList<>();
        if (a.flag("leak")) extra.add("--plant-leak");
        if (a.str("pool", "on").equals("off")) extra.addAll(List.of("--pool", "off"));
        Path out = Path.of(a.str("out", "results/local"));
        Files.createDirectories(out);
        StringBuilder csv = new StringBuilder("t_s,churned,heap_mb,direct_mb,direct_buffers,gcs,subscribers\n");
        try (Cluster c = new Cluster(Path.of("work/churn"), 1, jvm, extra)) {
            c.start(0);
            while (c.leader() != 0) Thread.sleep(50);
            System.out.println("broker pid " + c.pid(0));
            long start = System.nanoTime(), n = 0;
            int sampled = -1;
            boolean died = false;
            while (System.nanoTime() - start < seconds * 1_000_000_000L) {
                try (Subscriber s = new Subscriber("127.0.0.1:7100", 1, -1, null, (sh, q, p, ps, ts, b, to, tl, po, pl) -> {})) {
                    long until = System.nanoTime() + 2_000_000_000L;
                    while (!s.ready() && System.nanoTime() < until) s.poll();
                }
                n++;
                int sec = (int) ((System.nanoTime() - start) / 1_000_000_000L);
                if (sec != sampled) {
                    sampled = sec;
                    if (!c.alive(0) || Cluster.scrape(0) == null) {
                        died = true;
                        System.out.println("broker died after " + n + " connections, " + sec + " s");
                        break;
                    }
                    String line = String.format(Locale.ROOT, "%d,%d,%.1f,%.1f,%.0f,%.0f,%.0f", sec, n,
                            c.metric(0, "jvm_heap_used_bytes") / 1048576, c.metric(0, "jvm_direct_used_bytes") / 1048576,
                            c.metric(0, "jvm_direct_buffers"), c.metric(0, "jvm_gc_collections_total"), c.metric(0, "nanobus_subscribers"));
                    csv.append(line).append('\n');
                    if (sec % 5 == 0) System.out.println(line);
                }
                long next = start + n * 1_000_000_000L / perSecond;
                while (System.nanoTime() < next) Thread.sleep(1);
            }
            Files.writeString(out.resolve(tag + ".csv"), csv);
            if (died) {
                for (String l : Files.readAllLines(Path.of("work/churn/A.out"))) if (l.contains("OutOfMemory") || l.contains("fatal")) System.out.println(l);
            } else if (a.flag("jfr")) {
                c.stopGracefully(0);
            }
            return 0;
        }
    }
}
