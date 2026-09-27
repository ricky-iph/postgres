#!/bin/bash
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/lib.sh"

: "${PG_CONFIG:?set PG_CONFIG to the pg_config of the PostgreSQL 17 copy}"
PGBIN=$("$PG_CONFIG" --bindir)
PORT=$(free_port)
WORK=$(mktemp -d)
PGDATA="$WORK/data"

cleanup() {
  stop_pg >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT

(cd "$HERE/../extension" && cargo pgrx install --pg-config "$PG_CONFIG" >/dev/null)

"$PGBIN/initdb" -D "$PGDATA" -U postgres --auth-local=trust --auth-host=scram-sha-256 >/dev/null
cat >>"$PGDATA/postgresql.conf" <<EOF
shared_preload_libraries = 'pg_topics'
pg_topics.databases = ''
pg_topics.failover_is_fenced = on
EOF
start_pg >/dev/null

psql_as postgres "CREATE EXTENSION pg_topics" >/dev/null
psql_as postgres "SELECT topic.create_topic('public.orders_q', 1)" >/dev/null

stamp() {
  psql_as postgres "SELECT topic.stamp_topic('public', 'orders_q', 1)"
}

offset_of() {
  psql_as postgres "SELECT log_offset FROM public.orders_q WHERE value->>'s' = '$1'"
}

backlog_is_zero() {
  psql_as postgres "SELECT backlog_age = interval '0' FROM topic.topic_config WHERE topic = 'orders_q'"
}

psql_as postgres "SELECT topic.publish('public.orders_q', '{\"s\": \"A\"}')" >/dev/null
psql_as postgres "SELECT topic.publish('public.orders_q', '{\"s\": \"B\"}')" >/dev/null
chk "first stamp takes one row" 1 "$(stamp)"
chk "backlog_age shows the unstamped B" f "$(backlog_is_zero)"
chk "second stamp takes one row" 1 "$(stamp)"
chk "backlog_age is 0 with no unstamped row" t "$(backlog_is_zero)"
chk "A committed first and has offset 0" 0 "$(offset_of A)"
chk "B committed second and has offset 1" 1 "$(offset_of B)"

"$PGBIN/psql" -h /tmp -p "$PORT" -U postgres -d postgres -q >/dev/null 2>&1 <<'EOF' &
BEGIN;
SELECT topic.publish('public.orders_q', '{"s": "C"}');
SELECT pg_sleep(4);
COMMIT;
EOF
open_pid=$!
wait_for "[ \"\$(psql_as postgres \"SELECT count(*) FROM pg_stat_activity WHERE wait_event = 'PgSleep'\")\" = 1 ]"

psql_as postgres "SELECT topic.publish('public.orders_q', '{\"s\": \"D\"}')" >/dev/null
chk "stamp skips the open insert and takes D" 1 "$(stamp)"
chk "D has the next offset 2" 2 "$(offset_of D)"
chk "stamp finds nothing while C is open" 0 "$(stamp)"
chk "C has no offset while it is open" "" "$(offset_of C)"

wait "$open_pid"
chk "stamp takes C after its commit" 1 "$(stamp)"
chk "C has the next offset 3" 3 "$(offset_of C)"
chk "offsets have no gap" "0,1,2,3" \
  "$(psql_as postgres "SELECT string_agg(log_offset::text, ',' ORDER BY log_offset) FROM public.orders_q")"
chk "next_offset follows the last offset" 4 \
  "$(psql_as postgres "SELECT next_offset FROM topic.topic_band_position WHERE topic = 'orders_q'")"

"$PGBIN/pg_dump" -h /tmp -p "$PORT" -U postgres -d postgres -f "$WORK/dump.sql"
psql_as postgres "CREATE DATABASE restored" >/dev/null
restore_status=0
"$PGBIN/psql" -h /tmp -p "$PORT" -U postgres -d restored -v ON_ERROR_STOP=1 -q -f "$WORK/dump.sql" \
  >"$WORK/restore.log" 2>&1 || restore_status=$?
chk "restore runs with no error" 0 "$restore_status"

psql_restored() {
  "$PGBIN/psql" -h /tmp -p "$PORT" -U postgres -d restored -tAc "$1" 2>&1
}

chk "restore keeps the topic_config row" 1 \
  "$(psql_restored "SELECT count(*) FROM topic.topic_config WHERE topic = 'orders_q'")"
chk "restore keeps the stamped rows" 4 \
  "$(psql_restored "SELECT count(log_offset) FROM public.orders_q")"
chk "restore keeps next_offset" 4 \
  "$(psql_restored "SELECT next_offset FROM topic.topic_band_position WHERE topic = 'orders_q'")"
psql_restored "SELECT topic.publish('public.orders_q', '{\"s\": \"E\"}')" >/dev/null
chk "stamp after restore takes E" 1 "$(psql_restored "SELECT topic.stamp_topic('public', 'orders_q')")"
chk "E has the next offset 4" 4 \
  "$(psql_restored "SELECT log_offset FROM public.orders_q WHERE value->>'s' = 'E'")"

echo "passed: $pass  failed: $fail"
[ "$fail" -eq 0 ]
