package nanobus;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Owns the shards and decides whether this node leads. One control thread talks to the witness; the
 * shards only ever read the volatile role it publishes.
 */
public final class Broker implements AutoCloseable {
    private static final long TICK_MS = 50;

    final BrokerConfig cfg;
    final Shard[] shards;
    private final Thread[] threads;
    private final Thread control;
    private final Witness.Client witness;
    private final Path promiseFile;
    private final Metrics metrics;

    volatile boolean stopping;
    volatile Shard.Role role = new Shard.Role(Shard.Kind.STANDBY, 0);
    volatile boolean standbyInSet;
    volatile long takeovers;
    private int promised;
    private boolean addUnknown;

    public Broker(BrokerConfig cfg) throws IOException {
        this.cfg = cfg;
        Files.createDirectories(cfg.dir);
        promiseFile = cfg.dir.resolve("promised-epoch");
        if (Files.exists(promiseFile)) promised = Integer.parseInt(Files.readString(promiseFile).trim());
        witness = new Witness.Client(cfg.witnessHost, cfg.witnessPort, 200);
        shards = new Shard[cfg.shards];
        threads = new Thread[cfg.shards];
        for (int i = 0; i < cfg.shards; i++) shards[i] = new Shard(i, cfg, this);
        for (int i = 0; i < cfg.shards; i++) threads[i] = Thread.ofPlatform().name("shard-" + i).start(shards[i]);
        metrics = cfg.metricsPort > 0 ? new Metrics(this, threads) : null;
        control = Thread.ofPlatform().name("control").start(this::controlLoop);
    }

    // ---- promised epoch: never follow a leader older than one already followed (or led) ----

    synchronized int promised() { return promised; }

    synchronized boolean promise(int epoch) throws IOException {
        if (epoch < promised) return false;
        if (epoch > promised) {
            Witness.writeAtomic(promiseFile, Integer.toString(epoch));
            promised = epoch;
        }
        return true;
    }

    // ---- control loop ----

    private void controlLoop() {
        try {
            while (!stopping) {
                if (role.kind() == Shard.Kind.LEADER) leaderTick(); else standbyTick();
                Thread.sleep(TICK_MS);
            }
        } catch (InterruptedException e) {
            // closing
        } catch (Throwable t) {
            fatal("control", t);
        }
    }

    private void standbyTick() throws Exception {
        String[] st = witness.call("STATUS");
        if (st == null || !st[0].equals("OK")) return;
        boolean expired = st[3].equals("1");
        boolean eligible = st[4].equals("*") || List.of(st[4].split(",")).contains(cfg.id);
        if (!expired || !(eligible || !cfg.ackAll)) return;
        // Stop acking the old leader BEFORE asking for the lease: from here on it cannot commit anything
        // that this log does not already hold.
        become(Shard.Kind.FENCED, 0);
        String[] r = witness.call("ACQUIRE " + cfg.id + (cfg.ackAll ? "" : " force"));
        if (r != null && r[0].equals("OK")) {
            int epoch = Integer.parseInt(r[1]);
            promise(epoch);
            standbyInSet = addUnknown = false;
            takeovers++;
            become(Shard.Kind.LEADER, epoch);
            log(cfg.id, "leader, epoch " + epoch);
        } else {
            become(Shard.Kind.STANDBY, 0);
        }
    }

    private void leaderTick() throws Exception {
        int epoch = role.epoch();
        String[] r = witness.call("RENEW " + cfg.id + " " + epoch);
        // No answer is not a reason to stop: commits still need the standby, so staying up is safe.
        if (r == null) return;
        if (r[0].equals("NO")) {
            log(cfg.id, "lost epoch " + epoch + ", witness is at " + r[1]);
            standbyInSet = false;
            become(Shard.Kind.STANDBY, 0);
            return;
        }
        String version = r[2];
        boolean allSynced = true, anyLost = false;
        for (Shard s : shards) { allSynced &= s.mSynced; anyLost |= s.mLost; }
        if (anyLost || addUnknown) {
            if (standbyInSet || addUnknown) {
                r = change("REMOVE", epoch, version);
                if (r == null || !r[0].equals("OK")) return;       // cannot shrink the set: keep commits blocked
                standbyInSet = addUnknown = false;
                log(cfg.id, "standby removed from the in-sync set");
            }
            if (!anyLost) return;                                  // it is fine, it will be added again next tick
            for (Shard s : shards) { s.release = true; s.wakeup(); }
            for (Shard s : shards) while (s.release && !stopping) Thread.sleep(1);
        } else if (allSynced && !standbyInSet) {
            r = change("ADD", epoch, version);
            if (r == null) addUnknown = true;                      // it may have been applied: undo it first
            else if (r[0].equals("OK")) { standbyInSet = true; log(cfg.id, "standby in sync"); }
        }
    }

    private String[] change(String op, int epoch, String version) {
        return witness.call(op + " " + cfg.id + " " + epoch + " " + cfg.peerId + " " + version);
    }

    private void become(Shard.Kind kind, int epoch) throws InterruptedException {
        Shard.Role r = new Shard.Role(kind, epoch);
        role = r;
        for (Shard s : shards) s.wakeup();
        for (Shard s : shards) while (s.applied != r && !stopping) Thread.sleep(0, 200_000);
    }

    public boolean isLeader() { return role.kind() == Shard.Kind.LEADER; }
    public boolean standbyInSync() { return standbyInSet; }

    void fatal(String where, Throwable t) {
        if (stopping) return;
        t.printStackTrace();
        log(cfg.id, "fatal in " + where + ": " + t + " - exiting so the standby can take over");
        Runtime.getRuntime().halt(70);
    }

    static void log(String id, String msg) {
        System.out.printf("%tT.%<tL [%s] %s%n", System.currentTimeMillis(), id, msg);
    }

    @Override public void close() throws Exception {
        stopping = true;
        control.interrupt();
        control.join();
        for (Shard s : shards) s.wakeup();
        for (Thread t : threads) t.join();
        if (metrics != null) metrics.close();
    }
}
