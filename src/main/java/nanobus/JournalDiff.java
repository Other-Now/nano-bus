package nanobus;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import static nanobus.Proto.*;

/** Offline check: do two nodes hold the same records? */
public final class JournalDiff {
    private JournalDiff() {}

    public static String compare(Path a, Path b, int shards) throws IOException {
        long total = 0;
        for (int s = 0; s < shards; s++) {
            try (Journal x = new Journal(a.resolve("shard" + s), 8 << 20, 1000, false);
                 Journal y = new Journal(b.resolve("shard" + s), 8 << 20, 1000, false)) {
                long from = Math.max(x.firstSeq(), y.firstSeq()), to = Math.min(x.lastSeq(), y.lastSeq());
                if (x.lastSeq() != y.lastSeq()) {
                    return "different length in shard " + s + ": " + x.lastSeq() + " vs " + y.lastSeq();
                }
                Journal.Cursor cx = new Journal.Cursor(from), cy = new Journal.Cursor(from);
                ByteBuffer bx = ByteBuffer.allocateDirect(MAX_RECORD), by = ByteBuffer.allocateDirect(MAX_RECORD);
                for (long q = from; q <= to; q++) {
                    x.read(cx, q, bx.clear(), MAX_RECORD, null);
                    y.read(cy, q, by.clear(), MAX_RECORD, null);
                    if (!bx.flip().equals(by.flip())) return "shard " + s + " differs at seq " + q;
                    total++;
                }
            }
        }
        return "identical, " + total + " records";
    }
}
