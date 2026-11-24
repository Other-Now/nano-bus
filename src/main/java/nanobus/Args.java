package nanobus;

import java.util.HashMap;
import java.util.Map;

/** --key value, --key=value and bare --flag. */
public final class Args {
    private final Map<String, String> m = new HashMap<>();

    public Args(String[] argv, int from) {
        for (int i = from; i < argv.length; i++) {
            if (!argv[i].startsWith("--")) throw new IllegalArgumentException("unexpected " + argv[i]);
            String k = argv[i].substring(2);
            int eq = k.indexOf('=');
            if (eq >= 0) m.put(k.substring(0, eq), k.substring(eq + 1));
            else if (i + 1 < argv.length && !argv[i + 1].startsWith("--")) m.put(k, argv[++i]);
            else m.put(k, "true");
        }
    }

    public String str(String k, String d) { return m.getOrDefault(k, d); }
    public int num(String k, int d) { return m.containsKey(k) ? Integer.parseInt(m.get(k)) : d; }
    public boolean flag(String k) { return "true".equals(m.get(k)); }
}
