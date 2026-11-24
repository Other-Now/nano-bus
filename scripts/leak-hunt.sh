#!/bin/bash
# Finds the planted leak the way one would in production, using only tools that ship with the JDK.
# Run from a directory that contains nano-bus.jar; writes evidence to $1 (default: leak-evidence).
set -e
out=${1:-leak-evidence}
mkdir -p "$out" work
java -Xmx96m -cp nano-bus.jar nanobus.Main churn --leak --jfr --per-second 10 --seconds 38 --tag leak --out "$out" > "$out/churn.txt" &
harness=$!
for _ in $(seq 50); do pid=$(sed -n 's/^broker pid //p' "$out/churn.txt"); [ -n "$pid" ] && break; sleep 0.1; done

snap() {   # $1 = name
  jcmd "$pid" GC.class_histogram > "$out/histo-$1.txt"      # runs a full GC first, so only live objects count
  jcmd "$pid" VM.native_memory summary scale=KB > "$out/nmt-$1.txt"
  curl -s http://127.0.0.1:9101/metrics | grep -E '^(jvm_heap|jvm_direct|nanobus_subscribers)' > "$out/metrics-$1.txt"
}
sleep 4;  snap early
sleep 20; snap late
wait $harness || true
cp work/churn/leak.jfr "$out/"

{
  echo "== 1. metrics: heap is flat, direct memory is not, and no subscriber is connected"
  paste "$out/metrics-early.txt" "$out/metrics-late.txt" | awk '{printf "%-38s %14s -> %s\n", $1, $2, $4}'
  echo
  echo "== 2. native memory tracking: which JVM category grew (direct buffers are in Other)"
  for c in "Java Heap" Other Thread Class; do
    e=$(grep -m1 "$c (reserved" "$out/nmt-early.txt" | sed 's/.*committed=\([0-9]*\)KB.*/\1/')
    l=$(grep -m1 "$c (reserved" "$out/nmt-late.txt" | sed 's/.*committed=\([0-9]*\)KB.*/\1/')
    printf "%-12s committed %8s KB -> %8s KB\n" "$c" "$e" "$l"
  done
  echo
  echo "== 3. live objects that grew between two full GCs (count early -> late, shallow bytes late)"
  awk 'FNR==NR { if ($1 ~ /^[0-9]+:$/) e[$4]=$2; next }
       $1 ~ /^[0-9]+:$/ && $2-e[$4] >= 100 { printf "%-52s %7d -> %7d  %9d B\n", $4, e[$4], $2, $3 }' \
      "$out/histo-early.txt" "$out/histo-late.txt" | head -12
  echo
  echo "== 4. JFR old object samples: who holds them"
  jfr print --events jdk.OldObjectSample "$out/leak.jfr" > "$out/oldobjects.txt" || true
  grep -c "jdk.OldObjectSample" "$out/oldobjects.txt" || true
  awk '/^jdk.OldObjectSample/ {b=""} {b=b $0 "\n"} /^}/ { if (b ~ /sessionsById|nanobus.Conn/) { print b; n++ } if (n==1) exit }' "$out/oldobjects.txt" | head -60
} | tee "$out/REPORT.txt"
