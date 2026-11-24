"""Breaks one safety rule at a time in a scratch copy and runs the unit tests. A surviving mutant is a hole in the tests."""
import pathlib, shutil, subprocess, sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
WORK = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else ROOT / "work" / "mutants")
S, J, B, W = "Shard.java", "Journal.java", "Broker.java", "Witness.java"
MUTANTS = [
    ("commit does not wait for the standby", S, "cfg.ackAll && syncRequired ?", "cfg.ackAll && syncRequired && now < 0 ?"),
    ("subscribers see uncommitted records", S, "pump(c, commit);", "pump(c, journal.lastSeq());"),
    ("no producer dedup", S, "if (cfg.dedup && pseq <= last)", "if (cfg.dedup && pseq < 0)"),
    ("rejoining node keeps its divergent tail", S, "journal.truncateAfter(keep);", "keep = journal.lastSeq();"),
    ("standby is in sync as soon as it connects", S, "replica.ackSeq >= syncPoint", "replica.ackSeq >= -1"),
    ("lost standby is released without telling the witness", B, "if (standbyInSet || addUnknown) {", "if (false) {"),
    ("witness promotes a stale node", W, "eligible.equals(\"*\") || has(a[1])", "true || has(a[1])"),
    ("witness ignores the lease", W, "if (!expired ||", "if ("),
    ("witness accepts a late ADD", W, "|| version != Long.parseLong(a[4])", ""),
    ("witness forgets the set on restart", W, "eligible = p[2];", ""),
    ("takeover keeps the old eligible set", W, "leader = a[1];\n                eligible = a[1];", "leader = a[1];"),
    ("recovery skips the CRC", J, "|| !crcOk(p, len)) break;", ") break;"),
    ("torn tail is not cut", J, "s.ch.truncate(p);", ""),
    ("truncation ignores the epoch boundary", J, "return Math.min(myEnd, peerEnd);", "return peerEnd;"),
    ("producers are forgotten with their segment", J, "producers.decode(Files.readAllBytes(snapFile(baseOf(files.get(0)))), epochs);", "producers.clear(); epochs.clear();"),
    ("flush is skipped", J, "if (flushed == end) return;", "if (flushed <= end) return;"),
]

mvn = shutil.which("mvn") or shutil.which("mvn.cmd")
killed = 0
for name, f, a, b in MUTANTS:
    shutil.rmtree(WORK, ignore_errors=True)
    shutil.copytree(ROOT / "src", WORK / "src")
    shutil.copy(ROOT / "pom.xml", WORK)
    p = WORK / "src/main/java/nanobus" / f
    t = p.read_text(encoding="utf-8")
    assert t.count(a) == 1, f"{name}: pattern found {t.count(a)} times"
    p.write_text(t.replace(a, b), encoding="utf-8", newline="\n")
    try:
        r = subprocess.run([mvn, "-q", "-B", "-o", "test"], cwd=WORK, capture_output=True, text=True, timeout=240)
        dead = r.returncode != 0
        why = "COMPILE ERROR" if "COMPILATION" in r.stdout or "Compilation failure" in r.stdout else ""
    except subprocess.TimeoutExpired:
        dead, why = True, "timeout"
    killed += dead and why != "COMPILE ERROR"
    print(f"{'killed  ' if dead else 'SURVIVED'}  {name} {why}", flush=True)
print(f"{killed}/{len(MUTANTS)} killed")
