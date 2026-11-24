package nanobus;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Last accepted sequence per producer. Rebuilt from the journal, so it survives failover. */
final class ProducerTable {
    static final class State {
        final long id;
        long lastPseq;     // last producerSeq in the journal
        long lastLogSeq;   // journal seq of that record
        State(long id) { this.id = id; }
    }

    private final Map<Long, State> map = new HashMap<>();
    private State last;    // replication reads runs of the same producer: avoids boxing the key

    State get(long id) {
        State s = last;
        if (s != null && s.id == id) return s;
        return last = map.computeIfAbsent(id, State::new);
    }

    void clear() { map.clear(); last = null; }
    int size() { return map.size(); }

    /** Written next to every segment, so old segments can be deleted without forgetting producers. */
    byte[] encode(List<long[]> epochs) {
        ByteBuffer b = ByteBuffer.allocate(8 + map.size() * 24 + epochs.size() * 16);
        b.putInt(map.size()).putInt(epochs.size());
        for (State s : map.values()) b.putLong(s.id).putLong(s.lastPseq).putLong(s.lastLogSeq);
        for (long[] e : epochs) b.putLong(e[0]).putLong(e[1]);
        return b.array();
    }

    void decode(byte[] snap, List<long[]> epochs) {
        clear();
        epochs.clear();
        ByteBuffer b = ByteBuffer.wrap(snap);
        int n = b.getInt(), m = b.getInt();
        for (int i = 0; i < n; i++) {
            State s = get(b.getLong());
            s.lastPseq = b.getLong();
            s.lastLogSeq = b.getLong();
        }
        for (int i = 0; i < m; i++) epochs.add(new long[]{b.getLong(), b.getLong()});
    }
}
