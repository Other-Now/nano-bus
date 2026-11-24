package nanobus;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class WitnessTest {
    @TempDir Path dir;

    private static String version(Witness w, String id, int epoch) throws Exception {
        return w.handle("RENEW", id, "" + epoch).split(" ")[2];
    }

    @Test void leaseAndEligibility() throws Exception {
        try (Witness w = new Witness(0, dir, 100)) {
            assertEquals("OK 1", w.handle("ACQUIRE", "A"));
            assertEquals("NO 1", w.handle("ACQUIRE", "B"), "the lease is held");
            Thread.sleep(130);
            assertEquals("NO 1", w.handle("ACQUIRE", "B"), "expired, but B is not in the in-sync set");
            assertEquals("OK 1", w.handle("ADD", "A", "1", "B", version(w, "A", 1)));
            assertEquals("NO 1", w.handle("ACQUIRE", "B"), "in sync, but the lease is held");
            Thread.sleep(130);
            assertEquals("OK 2", w.handle("ACQUIRE", "B"));
            assertTrue(w.handle("RENEW", "A", "1").startsWith("NO"));
            assertEquals("NO 2", w.handle("REMOVE", "A", "1", "B", "2"), "an old leader cannot change the set");
            Thread.sleep(130);
            assertEquals("NO 2", w.handle("ACQUIRE", "A"), "the takeover shrank the set to B");
        }
    }

    @Test void aLateAddCannotUndoARemove() throws Exception {
        try (Witness w = new Witness(0, dir, 1000)) {
            w.handle("ACQUIRE", "A");
            String v = version(w, "A", 1);
            // the ADD with version v is stuck in the network; the leader gives up and removes
            assertTrue(w.handle("REMOVE", "A", "1", "B", v).startsWith("OK"));
            assertTrue(w.handle("ADD", "A", "1", "B", v).startsWith("NO"), "the delayed ADD arrives now");
            assertTrue(w.handle("STATUS").endsWith(" A"));
        }
    }

    @Test void restartKeepsEpochAndSetAndDoesNotExpireTheLease() throws Exception {
        try (Witness w = new Witness(0, dir, 200)) {
            w.handle("ACQUIRE", "A");
            w.handle("ADD", "A", "1", "B", version(w, "A", 1));
        }
        try (Witness w = new Witness(0, dir, 200)) {
            assertEquals("OK 1 A 0 A,B", w.handle("STATUS"));
            assertEquals("NO 1", w.handle("ACQUIRE", "B"), "the holder gets a full lease after a restart");
            Thread.sleep(230);
            assertEquals("OK 2", w.handle("ACQUIRE", "B"));
        }
    }
}
