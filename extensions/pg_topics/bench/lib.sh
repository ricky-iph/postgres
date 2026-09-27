pass=0
fail=0

chk() {
  if [ "$2" = "$3" ]; then
    echo "PASS  $1"
    pass=$((pass + 1))
  else
    echo "FAIL  $1"
    echo "        expected: [$2]"
    echo "        actual:   [$3]"
    fail=$((fail + 1))
  fi
}

start_pg() {
  "$PGBIN/pg_ctl" -D "$PGDATA" -l "$PGDATA/log" -o "-p $PORT -k /tmp" -w start
}

stop_pg() {
  "$PGBIN/pg_ctl" -D "$PGDATA" -m fast -w stop
}

kill9_pg() {
  local pid
  pid=$(head -1 "$PGDATA/postmaster.pid")
  kill -9 "$pid" $(pgrep -P "$pid")
}

free_port() {
  local port
  while port=$((20000 + RANDOM % 20000)); [ -e "/tmp/.s.PGSQL.$port" ] || (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; do
    :
  done
  echo "$port"
}

psql_as() {
  local role=$1 sql=$2
  "$PGBIN/psql" -h /tmp -p "$PORT" -U "$role" -d postgres -tAc "$sql" 2>&1
}

make_cert() {
  local dir=$1
  openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
    -keyout "$dir/server.key" -out "$dir/server.crt" \
    -subj /CN=localhost -addext subjectAltName=DNS:localhost,IP:127.0.0.1 2>&1
}

wait_for() {
  local i
  for i in $(seq 1 60); do
    eval "$1" && return 0
    sleep 1
  done
  return 1
}

STAMP_LOCK_NS=1885828211

cleanup() {
  kill $(jobs -p) 2>/dev/null || true
  stop_pg >/dev/null 2>&1 || true
  rm -rf "$WORK"
}

new_cluster() {
  : "${PG_CONFIG:?set PG_CONFIG to the pg_config of the PostgreSQL 17 copy}"
  PGBIN=$("$PG_CONFIG" --bindir)
  PORT=$(free_port)
  WORK=$(mktemp -d)
  PGDATA="$WORK/data"
  trap cleanup EXIT
  (cd "$HERE/../extension" && cargo pgrx install --pg-config "$PG_CONFIG" >/dev/null)
  "$PGBIN/initdb" -D "$PGDATA" -U postgres --auth-local=trust --auth-host=scram-sha-256 >/dev/null
  cat >>"$PGDATA/postgresql.conf" <<CONF
shared_preload_libraries = 'pg_topics'
pg_topics.databases = 'postgres'
pg_topics.failover_is_fenced = on
CONF
  start_pg >/dev/null
  psql_as postgres "CREATE EXTENSION pg_topics" >/dev/null
}

unstamped() {
  psql_as postgres "SELECT count(*) FROM $1 WHERE log_offset IS NULL"
}

hold_stamp_lock() {
  "$PGBIN/psql" -h /tmp -p "$PORT" -U postgres -d postgres -q -o /dev/null \
    -c "SELECT pg_advisory_lock($STAMP_LOCK_NS, hashtext('$1'))" -c "SELECT pg_sleep(3600)" >/dev/null 2>&1 &
  wait_for "[ \"\$(psql_as postgres \"SELECT count(*) FROM pg_stat_activity WHERE wait_event = 'PgSleep'\")\" = 1 ]"
}

release_stamp_lock() {
  psql_as postgres "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE wait_event = 'PgSleep'" >/dev/null
}

gap_free() {
  local t rows="" names=""
  for t in "$@"; do
    rows="$rows${rows:+ UNION ALL }SELECT '$t' AS t, band, log_offset FROM public.$t"
    names="$names${names:+, }'$t'"
  done
  psql_as postgres "SELECT bool_and(coalesce(s.n, 0) = p.next_offset AND coalesce(s.d, 0) = p.next_offset
                                    AND coalesce(s.hi, -1) = p.next_offset - 1)
    FROM topic.topic_band_position p
    LEFT JOIN (SELECT t, band, count(*) AS n, count(DISTINCT log_offset) AS d, max(log_offset) AS hi
               FROM ($rows) r GROUP BY t, band) s ON s.t = p.topic AND s.band = p.band
    WHERE p.schema_name = 'public' AND p.topic IN ($names)"
}
