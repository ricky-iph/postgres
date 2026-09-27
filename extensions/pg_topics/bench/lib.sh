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
  local pidfile="$PGDATA/postmaster.pid"
  [ -f "$pidfile" ] && kill -9 "$(head -1 "$pidfile")"
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
