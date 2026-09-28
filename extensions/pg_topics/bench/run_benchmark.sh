#!/bin/bash
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/lib.sh"
source "$HERE/benchmark/lib.sh"

BENCH_SECONDS=${BENCH_SECONDS:-120}
RESULTS=$(mktemp)
had_failure=0

row() { printf '%s\n' "$1" >>"$RESULTS"; }

fail_step() {
  echo "FAILED: $1"
  printf '%s\n' "$2"
  had_failure=1
}

produce_consume_round() {
  local label=$1 topic=$2 bands=$3 copies=$4 rate=$5 seconds=$6 short=$7
  local records=$((rate * seconds))
  local table=${topic#*.}
  : >"$WORK/backlog_$short"
  while sleep 1; do
    psql_as postgres "SELECT extract(epoch FROM backlog_age) FROM topic.stamp_backlog WHERE topic = '$table'" >>"$WORK/backlog_$short"
  done &
  local sampler=$!
  local raw produced=""
  if raw=$(kafka_java kafka-producer-perf-test --topic "$topic" --num-records "$records" --throughput "$rate" \
    --payload-file /w/payload.json --producer.config /w/alice.properties \
    --producer-props "bootstrap.servers=$BOOTSTRAP_HOST:$KPORT" acks=all 2>&1); then
    produced=$(grep 'records sent' <<<"$raw" | tail -n 1)
  fi
  kill "$sampler"
  wait "$sampler" 2>/dev/null || true
  if [ -z "$produced" ]; then
    fail_step "$label, produce" "$raw"
    return 0
  fi
  local max_backlog p99_backlog
  max_backlog=$(sort -g "$WORK/backlog_$short" | tail -n 1)
  p99_backlog=$(percentile 99 "$WORK/backlog_$short")
  echo "$label produce: $produced"
  echo "$label max backlog_age ${max_backlog}s, p99 backlog_age ${p99_backlog}s"

  local consumed=""
  if raw=$(kafka_java kafka-consumer-perf-test --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" --topic "$topic" --group "consume_$short" \
    --messages "$records" --timeout 60000 --consumer.config /w/alice.properties 2>&1); then
    consumed=$(consumer_row "$raw")
  fi
  if [ -z "$consumed" ]; then
    fail_step "$label, consume" "$raw"
    row "| $label | $bands | $copies | $rate | $(producer_rate "$produced") | $(producer_p50 "$produced") | $(producer_p99 "$produced") | $(producer_p999 "$produced") | FAILED | max ${max_backlog}s |"
  else
    echo "$label consume: $(consumer_rate "$consumed") records/s, $(consumer_mb_rate "$consumed") MB/s"
    row "| $label | $bands | $copies | $rate | $(producer_rate "$produced") | $(producer_p50 "$produced") | $(producer_p99 "$produced") | $(producer_p999 "$produced") | $(consumer_rate "$consumed") rec/s | max ${max_backlog}s |"
  fi

  : >"$WORK/e2e_$short"
  e2e_latency_samples "$topic" 10 >"$WORK/e2e_$short"
  local e2e_p50 e2e_p99 e2e_max e2e_n
  e2e_n=$(wc -l <"$WORK/e2e_$short")
  if [ "$e2e_n" -eq 0 ]; then
    fail_step "$label, end-to-end latency" "0 of 10 samples returned a latency"
    return 0
  fi
  e2e_p50=$(percentile 50 "$WORK/e2e_$short")
  e2e_p99=$(percentile 99 "$WORK/e2e_$short")
  e2e_max=$(sort -g "$WORK/e2e_$short" | tail -n 1)
  echo "$label end-to-end latency ms ($e2e_n of 10 samples): p50=$e2e_p50 p99=$e2e_p99 max=$e2e_max"
  row "| $label, end-to-end latency | $bands | $copies | - | - | $e2e_p50 | $e2e_p99 | - | - | - |"
}

echo "== group 1: single node =="
new_cluster
install_release_build
psql_as postgres "CREATE ROLE alice LOGIN PASSWORD 'alice-pw'; CREATE SCHEMA alice AUTHORIZATION alice" >/dev/null
start_listener
java_config alice alice-pw
write_payload_json

reachable=f
kafka_reachable && reachable=t
[ "$reachable" = f ] && echo "SKIPPED: docker cannot reach the listener (group 1, all Kafka scenarios)"

if [ "$reachable" = t ]; then
  for bands in 4 12; do
    topic="alice.b${bands}_q"
    psql_as alice "SELECT topic.create_topic('$topic', $bands)" >/dev/null
    produce_consume_round "produce+consume, fixed 10000 rec/s" "$topic" "$bands" 1 10000 "$BENCH_SECONDS" "b$bands"
  done

  max_records=200000
  topic2=alice.max1_q
  psql_as alice "SELECT topic.create_topic('$topic2', 4)" >/dev/null
  produced=""
  if raw=$(kafka_java kafka-producer-perf-test --topic "$topic2" --num-records "$max_records" --throughput -1 \
    --payload-file /w/payload.json --producer.config /w/alice.properties \
    --producer-props "bootstrap.servers=$BOOTSTRAP_HOST:$KPORT" acks=all 2>&1); then
    produced=$(grep 'records sent' <<<"$raw" | tail -n 1)
  fi
  if [ -z "$produced" ]; then
    fail_step "max throughput, 1 producer" "$raw"
  else
    echo "max throughput, 1 producer: $produced"
    row "| max throughput, 1 producer | 4 | 1 | -1 | $(producer_rate "$produced") | $(producer_p50 "$produced") | $(producer_p99 "$produced") | $(producer_p999 "$produced") | - | - |"
  fi

  topic2b=alice.max4_q
  psql_as alice "SELECT topic.create_topic('$topic2b', 4)" >/dev/null
  per_producer=$((max_records / 4))
  pids=()
  for n in 1 2 3 4; do
    kafka_java kafka-producer-perf-test --topic "$topic2b" --num-records "$per_producer" --throughput -1 \
      --payload-file /w/payload.json --producer.config /w/alice.properties \
      --producer-props "bootstrap.servers=$BOOTSTRAP_HOST:$KPORT" acks=all >"$WORK/max4_$n.out" 2>&1 &
    pids+=("$!")
  done
  for pid in "${pids[@]}"; do wait "$pid" || true; done
  total_rate=0
  worst_p99=0
  all_producers_ok=t
  for n in 1 2 3 4; do
    line=$(grep 'records sent' "$WORK/max4_$n.out" | tail -n 1)
    if [ -z "$line" ]; then
      fail_step "max throughput, 4 producers (producer $n)" "$(cat "$WORK/max4_$n.out")"
      all_producers_ok=f
      continue
    fi
    r=$(producer_rate "$line")
    p=$(producer_p99 "$line")
    total_rate=$(awk -v a="$total_rate" -v b="$r" 'BEGIN { printf "%.1f", a + b }')
    worse=$(awk -v a="$p" -v b="$worst_p99" 'BEGIN { print (a + 0 > b + 0) ? 1 : 0 }')
    [ "$worse" = 1 ] && worst_p99=$p
  done
  if [ "$all_producers_ok" = t ]; then
    echo "max throughput, 4 producers: aggregate $total_rate records/s, worst p99 ${worst_p99} ms"
    row "| max throughput, 4 producers | 4 | 1 | -1 | $total_rate | - | $worst_p99 | - | - | - |"
  fi

fi
stop_pg
rm -rf "$WORK"
trap - EXIT

echo "== group 5: three nodes, one cluster =="
new_cluster
install_release_build
P1=$PORT
D1=$PGDATA
K1=$(free_port)
D2=$WORK/s1
D3=$WORK/s2
trap 'PGDATA=$D3 stop_pg >/dev/null 2>&1 || true; PGDATA=$D2 stop_pg >/dev/null 2>&1 || true; cleanup' EXIT

on() {
  local port=$1
  shift
  PORT=$port psql_as "$@"
}

build_replica() {
  local name=$1 data_dir=$2
  local port kport
  port=$(free_port)
  kport=$(free_port)
  "$PGBIN/pg_basebackup" -h /tmp -p "$P1" -U postgres -D "$data_dir" -R -X stream -c fast >/dev/null
  echo "primary_conninfo = 'host=/tmp port=$P1 user=postgres application_name=$name'" >>"$data_dir/postgresql.auto.conf"
  echo "pg_topics.port = $kport" >>"$data_dir/postgresql.conf"
  echo "$port $kport"
}

make_cert "$WORK" >/dev/null
stop_pg
cat >>"$D1/postgresql.conf" <<CONF
pg_topics.tls_cert_file = '$WORK/server.crt'
pg_topics.tls_key_file = '$WORK/server.key'
pg_topics.advertised_host = '$BOOTSTRAP_HOST'
pg_topics.port = $K1
CONF
start_pg
wait_for "[ \"\$(listener_status)\" = 'listening on port $K1' ]"
KPORT=$K1
docker build -q -t pg_topics_python:2.15.1 "$HERE/clients/python" >/dev/null 2>&1 || { echo "FAIL  docker build of clients/python"; exit 1; }

on "$P1" postgres "CREATE ROLE alice LOGIN PASSWORD 'alice-pw'; CREATE SCHEMA alice AUTHORIZATION alice" >/dev/null
java_config alice alice-pw
write_payload_json

read -r P2 K2 <<<"$(build_replica s1 "$D2")"
read -r P3 K3 <<<"$(build_replica s2 "$D3")"
PGDATA=$D2 PORT=$P2 start_pg >/dev/null
PGDATA=$D3 PORT=$P3 start_pg >/dev/null
wait_for "[ \"\$(on "$P2" postgres 'SELECT pg_is_in_recovery()')\" = t ]"
wait_for "[ \"\$(on "$P3" postgres 'SELECT pg_is_in_recovery()')\" = t ]"
wait_for "[ \"\$(on "$P1" postgres \"SELECT count(*) FROM pg_stat_replication WHERE application_name IN ('s1', 's2')\")\" = 2 ]"

reachable5=f
kafka_reachable && reachable5=t
[ "$reachable5" = f ] && echo "SKIPPED: docker cannot reach the listener (group 5, Kafka produce and consume)"

configs=(
  "FIRST 2 (s1, s2)|first2|3 (FIRST 2, all copies)"
  "ANY 1 (s1, s2)|any1|3 (ANY 1, 2-of-3 quorum)"
)
for entry in "${configs[@]}"; do
  IFS='|' read -r sync_value slug copies_desc <<<"$entry"
  on "$P1" postgres "ALTER SYSTEM SET synchronous_standby_names = '$sync_value'" >/dev/null
  on "$P1" postgres "SELECT pg_reload_conf()" >/dev/null
  wait_for "[ \"\$(on "$P1" postgres 'SHOW synchronous_standby_names')\" = '$sync_value' ]"
  topic="public.bench5_${slug}_q"
  on "$P1" postgres "SELECT topic.create_topic('$topic', 4, min_durability => 'replicated')" >/dev/null
  on "$P1" postgres "SELECT topic.grant_publish('$topic', 'alice')" >/dev/null
  on "$P1" postgres "SELECT topic.grant_consume('$topic', 'alice')" >/dev/null

  if [ "$reachable5" = t ]; then
    produce_consume_round "3-node $copies_desc" "$topic" 4 "$copies_desc" 10000 "$BENCH_SECONDS" "$slug"
  fi

  echo "SELECT topic.publish('$topic', '{\"v\": 1}');" >"$WORK/pub_$slug.sql"
  for clients in 1 4 8; do
    before=$(on "$P1" postgres "SELECT count(log_offset) FROM $topic")
    tps=""
    if raw=$(PGOPTIONS="-c synchronous_commit=remote_apply" "$PGBIN/pgbench" -h /tmp -p "$P1" -U postgres -n \
      -c "$clients" -j "$clients" -T 10 -f "$WORK/pub_$slug.sql" postgres 2>&1); then
      tps=$(sed -n 's/^tps = \([0-9.]*\) .*/\1/p' <<<"$raw")
    fi
    if [ -z "$tps" ]; then
      fail_step "3-node $copies_desc, SQL publish, $clients clients" "$raw"
      continue
    fi
    after=$(on "$P1" postgres "SELECT count(log_offset) FROM $topic")
    echo "3-node $copies_desc, SQL publish, $clients clients: tps=$tps stamped/s=$(( (after - before) / 10 ))"
    row "| 3-node $copies_desc, SQL publish ($clients clients) | 4 | $copies_desc | - | $tps | - | - | - | - | - |"
  done
done

echo "== group 6: SQL publish, single node =="
if sql_out=$(bash "$HERE/run_publish_throughput.sh" 2>&1); then
  :
else
  fail_step "group 6, SQL publish single node" "$sql_out"
fi
echo "$sql_out"
while read -r durability clients tps stamped backlog; do
  row "| SQL publish (durable), $clients clients | - | 1 | - | $tps | - | - | - | ${stamped}/s | ${backlog}s |"
done < <(awk '$1 == "durable" { print $1, $2, $3, $4, $5 }' <<<"$sql_out")

echo
echo "== summary =="
echo "| Scenario | Bands | Copies | Target rate (rec/s) | Achieved rate (rec/s) | p50 (ms) | p99 (ms) | p99.9 (ms) | Consume rate | Max backlog_age |"
echo "|---|---|---|---|---|---|---|---|---|---|"
cat "$RESULTS"
rm -f "$RESULTS"
echo
machine_facts release
echo "Caveat: every node in the 3-node scenario runs on this one host, so replication adds no network latency."
[ "$had_failure" -eq 0 ]
