package nanobus.harness;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** A witness and two brokers as real JVMs, so that kill -9 means kill -9. */
final class Cluster implements AutoCloseable {
    static final String BROKERS = "127.0.0.1:7100,127.0.0.1:7200";
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(500)).build();

    final Path dir;
    final int shards;
    private final List<String> jvm, extra;
    private Process witness;
    private final Process[] node = new Process[2];

    Cluster(Path dir, int shards, List<String> jvm, List<String> extra) throws IOException {
        this.dir = dir;
        this.shards = shards;
        this.jvm = jvm;
        this.extra = extra;
        wipe(dir);
        Files.createDirectories(dir);
        witness = spawn("witness", List.of("-Xmx32m"), List.of("witness", "--port", "7000", "--dir", dir.resolve("w").toString()));
        Runtime.getRuntime().addShutdownHook(new Thread(this::killAll));
    }

    static void wipe(Path dir) throws IOException {
        if (!Files.exists(dir)) return;
        try (var st = Files.walk(dir)) {
            for (Path p : st.sorted(Comparator.reverseOrder()).toList()) Files.delete(p);
        }
    }

    static String name(int i) { return i == 0 ? "A" : "B"; }

    void start(int i) throws IOException {
        List<String> a = new ArrayList<>(List.of("broker", "--id", name(i), "--port", i == 0 ? "7100" : "7200",
                "--peer", "127.0.0.1:" + (i == 0 ? "7200" : "7100"), "--witness", "127.0.0.1:7000",
                "--metrics", "910" + (i + 1), "--shards", "" + shards, "--dir", dir.resolve(name(i)).toString()));
        a.addAll(extra);
        List<String> j = new ArrayList<>();
        for (String s : jvm) j.add(s.replace("%n", name(i)));
        node[i] = spawn(name(i), j, a);
    }

    private Process spawn(String log, List<String> jvmArgs, List<String> args) throws IOException {
        List<String> cmd = new ArrayList<>();
        cmd.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        cmd.addAll(jvmArgs);
        cmd.add("-cp");
        cmd.add(System.getProperty("java.class.path"));
        cmd.add("nanobus.Main");
        cmd.addAll(args);
        File f = dir.resolve(log + ".out").toFile();
        return new ProcessBuilder(cmd).redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.appendTo(f)).start();
    }

    void kill9(int i) throws InterruptedException {
        node[i].destroyForcibly();
        node[i].waitFor();
    }

    void killWitness() throws InterruptedException { witness.destroyForcibly(); witness.waitFor(); }

    void startWitness() throws IOException {
        witness = spawn("witness", List.of("-Xmx32m"), List.of("witness", "--port", "7000", "--dir", dir.resolve("w").toString()));
    }

    long pid(int i) { return node[i].pid(); }
    boolean alive(int i) { return node[i] != null && node[i].isAlive(); }

    void signal(int i, String sig) throws IOException, InterruptedException {
        new ProcessBuilder("kill", "-" + sig, "" + pid(i)).start().waitFor();
    }

    void stopGracefully(int i) throws InterruptedException {
        if (!alive(i)) return;
        node[i].destroy();
        node[i].waitFor();
    }

    /** All samples of one metric, summed; -1 if the node does not answer. */
    double metric(int i, String name) {
        String body = scrape(i);
        if (body == null) return -1;
        double sum = 0;
        for (String l : body.split("\n")) {
            if (l.startsWith(name) && (l.charAt(name.length()) == ' ' || l.charAt(name.length()) == '{')) {
                sum += Double.parseDouble(l.substring(l.lastIndexOf(' ') + 1));
            }
        }
        return sum;
    }

    static String scrape(int i) {
        try {
            return HTTP.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:910" + (i + 1) + "/metrics"))
                    .timeout(Duration.ofSeconds(2)).build(), HttpResponse.BodyHandlers.ofString()).body();
        } catch (Exception e) {
            return null;
        }
    }

    int leader() {
        for (int i = 0; i < 2; i++) if (alive(i) && metric(i, "nanobus_leader") == 1) return i;
        return -1;
    }

    /** Waits for a leader whose standby is in sync. Returns the leader, or -1 on timeout. */
    int awaitInSync(int timeoutMs) throws InterruptedException {
        long end = System.nanoTime() + timeoutMs * 1_000_000L;
        while (System.nanoTime() < end) {
            for (int i = 0; i < 2; i++) {
                if (alive(i) && metric(i, "nanobus_leader") == 1 && metric(i, "nanobus_standby_in_sync") == 1) return i;
            }
            Thread.sleep(50);
        }
        return -1;
    }

    private void killAll() {
        for (Process p : node) if (p != null) p.destroyForcibly();
        witness.destroyForcibly();
    }

    @Override public void close() throws Exception {
        killAll();
        for (Process p : node) if (p != null) p.waitFor();
        witness.waitFor();
    }
}
