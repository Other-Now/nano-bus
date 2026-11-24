#!/bin/bash
# Real Prometheus, real brokers: measures how long each fault takes to become a firing alert.
# Needs `prometheus` on PATH. Run from the repository root after `mvn package`.
set -e
jar=$PWD/target/nano-bus.jar
out=${1:-results/ci}
mkdir -p "$out"
rm -rf work/alert && mkdir -p work/alert
J="java -Xmx64m -XX:MaxDirectMemorySize=128m -cp $jar nanobus.Main"
now() { date +%s%3N; }
broker() { $J broker --id "$1" --port "$2" --peer "127.0.0.1:$3" --metrics "$4" --dir "work/alert/$1" > "work/alert/$1.out" 2>&1 & }
$J witness --dir work/alert/w > /dev/null &
broker A 7100 7200 9101; A=$!
sleep 1.5
broker B 7200 7100 9102; B=$!
prometheus --config.file=monitoring/prometheus.yml --storage.tsdb.path=work/alert/tsdb --web.listen-address=127.0.0.1:9090 > work/alert/prom.out 2>&1 &
trap 'kill $(jobs -p) 2>/dev/null' EXIT

firing() { curl -s http://127.0.0.1:9090/api/v1/alerts | python3 -c '
import json,sys
print(" ".join(a["labels"]["alertname"]+":"+a["labels"].get("topic","") for a in json.load(sys.stdin)["data"]["alerts"] if a["state"]=="firing"))'; }
await() {   # $1 = alert, $2 = fault time in ms
  for _ in $(seq 600); do
    if firing | grep -q "$1"; then echo "$1 fired $(( $(now) - $2 )) ms after the fault" | tee -a "$out/alerts.txt"; return; fi
    sleep 0.1
  done
  echo "$1 never fired"; exit 1
}
: > "$out/alerts.txt"
sleep 3
$J stall --shards 2 --quiet-after 25 --stop-reading-after 25 --seconds 110 > work/alert/stall.out &
sleep 20
f=$(firing); [ -z "$f" ] && echo "healthy cluster under load: no alert firing" | tee -a "$out/alerts.txt" || { echo "unexpected: $f"; exit 1; }
while ! grep -q STALL_AT work/alert/stall.out; do sleep 0.1; done
t=$(sed -n 's/STALL_AT //p' work/alert/stall.out)
await "StalledPublisher:md.NSE" "$t"
firing | grep -q "StalledPublisher:md.BSE" && { echo "false alarm on the busy topic"; exit 1; }
await ConsumerLagging "$t"
t=$(now); kill -9 "$B"; await StandbyOutOfSync "$t"
t=$(now); kill -9 "$A"; await NoLeader "$t"
