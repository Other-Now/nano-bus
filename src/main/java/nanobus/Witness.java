package nanobus;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The third vote. Two brokers cannot tell "peer is dead" from "link is down", so a small third process
 * holds one record: (epoch, leader, eligible set, lease). It never sees a message.
 *
 *  - ACQUIRE is granted only when the lease has run out AND the caller is in the eligible set.
 *    It bumps the epoch and shrinks the set to the caller.
 *  - The leader ADDs the standby once it holds every committed record, and must REMOVE it before
 *    committing anything without it. So whoever is in the set has every committed record.
 *
 * The lease only decides WHEN a takeover may happen. Safety comes from the eligible set and the epoch,
 * so a wrong clock or a long pause can delay a failover but cannot lose an acked message.
 */
public final class Witness implements AutoCloseable {
    private final Path file;
    private final long ttlNs;
    private final ServerSocket server;
    private int epoch;
    private String leader = "-";
    private String eligible = "*";          // "*" = brand-new cluster, both logs are empty
    private long version;                   // bumped by every change of the set, see ADD/REMOVE
    private long leaseUntil;

    public Witness(int port, Path dir, int ttlMs) throws IOException {
        this.ttlNs = ttlMs * 1_000_000L;
        Files.createDirectories(dir);
        file = dir.resolve("witness");
        if (Files.exists(file)) {
            String[] p = Files.readString(file).trim().split(" ");
            epoch = Integer.parseInt(p[0]);
            leader = p[1];
            eligible = p[2];
            version = Long.parseLong(p[3]);
        }
        // After a restart the old lease is unknown, so give its holder a full one.
        leaseUntil = System.nanoTime() + (epoch == 0 ? 0 : ttlNs);
        server = new ServerSocket(port, 50, InetAddress.getByName("127.0.0.1"));
        Thread.ofPlatform().daemon().name("witness").start(this::acceptLoop);
    }

    public int port() { return server.getLocalPort(); }

    private void acceptLoop() {
        while (!server.isClosed()) {
            try {
                Socket s = server.accept();
                Thread.ofPlatform().daemon().start(() -> serve(s));
            } catch (IOException e) {
                return;
            }
        }
    }

    private final Set<Socket> clients = ConcurrentHashMap.newKeySet();

    /** Test hook: cut every broker off. */
    void dropClients() throws IOException { for (Socket s : clients) s.close(); }

    private void serve(Socket s) {
        clients.add(s);
        try (s; var in = new BufferedReader(new InputStreamReader(s.getInputStream()));
             var out = new PrintWriter(s.getOutputStream(), true)) {
            s.setTcpNoDelay(true);
            for (String line; (line = in.readLine()) != null; ) out.println(handle(line.split(" ")));
        } catch (IOException | RuntimeException ignored) {
            // the broker went away
        } finally {
            clients.remove(s);
        }
    }

    synchronized String handle(String... a) throws IOException {
        long now = System.nanoTime();
        boolean expired = now - leaseUntil >= 0;
        switch (a[0]) {
            case "STATUS":
                return "OK " + epoch + " " + leader + " " + (expired ? 1 : 0) + " " + eligible;
            case "ACQUIRE":       // "force" exists only for the naive control in the chaos run
                if (!expired || !(eligible.equals("*") || has(a[1]) || a.length > 2)) return "NO " + epoch;
                epoch++;
                version++;
                leader = a[1];
                eligible = a[1];
                persist();
                leaseUntil = System.nanoTime() + ttlNs;
                return "OK " + epoch;
            case "RENEW":
                if (!holds(a[1], a[2])) return "NO " + epoch;
                leaseUntil = now + ttlNs;
                return "OK " + epoch + " " + version;
            case "ADD":
            case "REMOVE": {
                // The version makes this a compare-and-set: an ADD that timed out and arrives late, after a
                // REMOVE, must not put a stale standby back into the set.
                if (!holds(a[1], a[2]) || version != Long.parseLong(a[4])) return "NO " + epoch;
                TreeSet<String> set = new TreeSet<>(List.of(eligible.split(",")));
                if (a[0].equals("ADD")) set.add(a[3]); else set.remove(a[3]);
                eligible = String.join(",", set);
                version++;
                persist();
                return "OK " + epoch;
            }
            default:
                return "ERR";
        }
    }

    private boolean holds(String id, String e) { return leader.equals(id) && epoch == Integer.parseInt(e); }

    private boolean has(String id) {
        for (String s : eligible.split(",")) if (s.equals(id)) return true;
        return false;
    }

    private void persist() throws IOException { writeAtomic(file, epoch + " " + leader + " " + eligible + " " + version); }

    static void writeAtomic(Path f, String text) throws IOException {
        Path tmp = f.resolveSibling(f.getFileName() + ".tmp");
        try (FileChannel c = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            c.write(ByteBuffer.wrap(text.getBytes()));
            c.force(true);
        }
        Files.move(tmp, f, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    @Override public void close() throws IOException { server.close(); }

    /** Blocking client with a short timeout. Any failure returns null: "no answer" is not "no". */
    public static final class Client {
        private final InetSocketAddress addr;
        private final int timeoutMs;
        private Socket s;
        private BufferedReader in;
        private PrintWriter out;

        public Client(String host, int port, int timeoutMs) {
            this.addr = new InetSocketAddress(host, port);
            this.timeoutMs = timeoutMs;
        }

        public String[] call(String cmd) {
            try {
                if (s == null) {
                    Socket n = new Socket();
                    s = n;
                    n.setTcpNoDelay(true);
                    n.connect(addr, timeoutMs);
                    n.setSoTimeout(timeoutMs);
                    in = new BufferedReader(new InputStreamReader(n.getInputStream()));
                    out = new PrintWriter(n.getOutputStream(), true);
                }
                out.println(cmd);
                String r = in.readLine();
                if (r == null) throw new IOException("closed");
                return r.split(" ");
            } catch (IOException e) {
                try { s.close(); } catch (IOException ignored) { /* already broken */ }
                s = null;       // a late reply must never be read as the answer to the next command
                return null;
            }
        }
    }
}
