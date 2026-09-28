# pg_topics

`pg_topics` is a PostgreSQL 17 extension. It turns a table into a Kafka topic.
A row you insert gets a log offset, in order, per band (Kafka calls a band a
partition). A stock Kafka client — Java, librdkafka, Confluent JS — can
produce and consume over the wire, on a port the extension opens. A SQL user
can do the same work with plain functions, inside their own transaction. One
optional worker keeps a normal table in sync with the topic, so you can also
just read the current state with `SELECT`. There is no separate broker to
run, back up, or patch.

## Limits

Three things to know before you start.

| Limit | Detail |
|---|---|
| A record value is JSON | A value that does not parse as JSON is refused. |
| A record key is UTF-8 text, 40 characters or fewer | A longer key, or a key that is not valid UTF-8, is refused. |
| A topic name is `schema.table_q` | The table name must end in `_q`. |

Warning: this is a rename for a Kafka user. A producer aimed at a topic named
`orders` gets `UNKNOWN_TOPIC_OR_PARTITION`. The real topic name is
`public.orders_q`. Point the client at the table name, not the old topic name.

## Install

Build the extension with `cargo-pgrx` 0.12.9, against a PostgreSQL 17 install:

```bash
cargo install cargo-pgrx --version 0.12.9 --locked
cd extensions/pg_topics/extension
cargo pgrx install --release --pg-config /path/to/pg_config
```

Add these lines to `postgresql.conf`, then restart PostgreSQL:

```ini
shared_preload_libraries = 'pg_topics'
pg_topics.databases = 'app'
pg_topics.failover_is_fenced = on
```

`pg_topics.databases` names every database that gets topics, as a comma list.
List only databases that exist. A missing one makes its four workers restart
every 5 seconds. Each named database starts four background workers: a
stamper, a partition worker, a sync worker, and the Kafka listener. Set
`max_worker_processes` to at least 4 times the number of named databases,
plus what the rest of the cluster already uses. `pg_topics.failover_is_fenced
= on` tells pg_topics that the operator fences the old primary on failover.
A topic may not ask for `durable` or `replicated` until this is `on`.

Then, in each named database:

```sql
CREATE EXTENSION pg_topics;
```

### `pg_hba.conf`

Two workers open a real network connection, so `pg_hba.conf` must allow them.

- The partition worker connects over the local Unix socket, as the bootstrap
  superuser (usually `postgres`). It needs a `local` line that lets that role
  in, the same line most clusters already have for local admin work.
- The Kafka listener connects to `127.0.0.1` over TCP, once per Kafka client,
  as that client's own Postgres role and password. It needs a `host` line for
  `127.0.0.1/32` with a password method, for example:

  ```
  host    all    all    127.0.0.1/32    scram-sha-256
  ```

  A `trust` or `peer` line for `127.0.0.1` does not work. The listener checks
  how Postgres authenticated the connection, and refuses anything that is not
  a password method. This stops a `trust` line from letting a Kafka client
  become any role it names.

### TLS

The Kafka listener needs TLS. Set one of these:

```ini
pg_topics.tls_cert_file = '/path/to/server.crt'
pg_topics.tls_key_file  = '/path/to/server.key'
```

Or, when the cluster already has `ssl = on` with its own certificate:

```ini
pg_topics.tls_use_postgres_cert = on
```

With neither set, the listener does not bind. `topic.health()` reports it
as not bound (see [Monitoring](#monitoring)).

### Every setting, with its default

| Setting | Default | Meaning |
|---|---|---|
| `pg_topics.databases` | (none) | The databases that get the four workers. A comma list. Needs a restart. |
| `pg_topics.failover_is_fenced` | `off` | The operator has fenced the old primary on failover. A topic may not ask for `durable` or `replicated` until this is `on`. |
| `pg_topics.port` | `9092` | The Kafka listener's TCP port. `0` means no listener. Set it per database with `ALTER DATABASE ... SET`. That setting replicates, so a standby on the same host must set its own port in its `postgresql.conf`. |
| `pg_topics.advertised_host` | `localhost` | The host name `Metadata` gives to a Kafka client for reconnecting. Set it to a reachable name. |
| `pg_topics.max_clients` | `100` | The most Kafka clients the listener accepts at once. Each one holds a Postgres connection; keep `max_connections` above this. |
| `pg_topics.max_message_bytes` | `1048576` | The largest record batch `Produce` accepts. A larger batch gets `MESSAGE_TOO_LARGE`. |
| `pg_topics.tls_cert_file` | (none) | The PEM certificate file for the listener. |
| `pg_topics.tls_key_file` | (none) | The PEM private key file for the listener. |
| `pg_topics.tls_use_postgres_cert` | `off` | Use the cluster's own `ssl_cert_file`/`ssl_key_file` instead. |
| `pg_topics.group_min_session_ms` | `6000` | The shortest session timeout a consumer group member may ask for. |
| `pg_topics.group_max_session_ms` | `1800000` | The longest session timeout a consumer group member may ask for. |
| `pg_topics.group_initial_rebalance_delay_ms` | `3000` | How long a brand new group's first join window stays open. |

A release of `pg_topics` needs a restart, because the four workers load at
postmaster start. Plan for every consumer group in every named database to
rebalance once, after the restart.

## SQL quick start

Create a topic and publish to it:

```sql
SELECT topic.create_topic('public.orders_q', band_count => 4);
SELECT topic.publish('public.orders_q', jsonb_build_object('order_id', 1), key => 'cust-42');
```

`publish` runs as a normal `INSERT` under the hood, so it commits with the
rest of your transaction, or not at all.

Attach a topic to a table you already have, so a background worker keeps that
table current:

```sql
SELECT topic.attach('public.orders'::regclass, sync_key => 'order_id');
```

Or create the table and the topic together, from a column list:

```sql
SELECT topic.create_table_topic('public.orders',
    columns    => '{"order_id": "int", "status": "text"}',
    sync_key   => 'order_id');
```

Read history directly, in SQL, with no consumer group:

```sql
SELECT * FROM topic.fetch('public.orders_q', band => 0, from_offset => 0);
SELECT * FROM topic.band_offsets('public.orders_q');
```

`fetch` returns the record headers as they are stored: a JSON array in the
order of the Kafka record, for example
`[{"key": "a", "value": "1"}, {"key": "bin", "value_base64": "/wAB"}, {"key": "n", "value": null}]`.
A value that is valid UTF-8 is text in `value`. Any other value is standard
base64 in `value_base64`. A null value is `"value": null`. One name can occur
more than one time. Give `publish` headers in the same format, and a Kafka
consumer gets the same bytes.

Commit a position without joining a group first, the way a simple SQL reader
does:

```sql
SELECT topic.commit_offset('public.orders_q', 'shippers', 0, 10, -1);
SELECT topic.fetch_offset('public.orders_q', 'shippers', 0);
```

Or join a group properly, the way a Kafka consumer group does:

```sql
SELECT * FROM topic.group_join('shippers', '', 'client-1', 30000, 5000,
    'consumer', '[{"name": "range"}]');
```

Let another role publish or read, without giving it the whole table:

```sql
SELECT topic.grant_publish('public.orders_q', 'order_service');
SELECT topic.grant_consume('public.orders_q', 'billing_service');
```

## Kafka client settings

Point a stock Kafka client at `pg_topics.advertised_host`, on
`pg_topics.port`. Every client needs:

| Setting | Value |
|---|---|
| `security.protocol` | `SASL_SSL` |
| `sasl.mechanism` | `PLAIN` |
| `sasl.username` / `sasl.password` | A Postgres role and its password |
| The CA file | The listener's own certificate, since it is usually self-signed |

Java reads the CA file through a truststore
(`ssl.truststore.type=PEM`, `ssl.truststore.location=...`). librdkafka and the
Confluent JS client read it directly (`ssl.ca.location=...`).

Warning: set librdkafka's `partitioner` to `murmur2_random`. Its default
partitioner does not agree with the one `pg_topics` uses for a keyed SQL
`publish`, so a key would land in a different band depending on which side
published it. The default partitioners of Java, kafkajs 2.x and franz-go
already agree.

An idempotent producer (`enable.idempotence=true`, the Java client's default
since version 3.0) works: a retried batch does not write a second row. Kafka
transactions are not supported; `InitTransactions` and related calls are
refused.

### Replication factor

`CreateTopics` accepts a replication factor N only when Postgres really keeps
N copies of each commit: the primary, plus the synchronous standbys that must
confirm each commit. `synchronous_standby_names` sets that number of standbys,
k:

| `synchronous_standby_names` | k | Largest replication factor |
|---|---|---|
| empty | 0 | 1 |
| `s1` or `s1, s2` (a plain list) | 1 | 2 |
| `2 (s1, s2)` or `FIRST 2 (s1, s2)` | 2 | 3 |
| `ANY 1 (s1, s2)` | 1 | 2 |
| `ANY 2 (s1, s2, s3)` | 2 | 3 |

- A replication factor of 1 or -1 makes a topic as before.
- A replication factor N from 2 to 1 + k makes the topic with
  `min_durability` `replicated`. Each commit then waits until k standbys
  apply it (`synchronous_commit = remote_apply`). This applies to every
  `acks` value and to SQL publishers. It also needs
  `pg_topics.failover_is_fenced = on`.
- Any other N gets `INVALID_REPLICATION_FACTOR`. The message gives the number
  of copies that the standby setup keeps.
- N above 1 with `pg_topics.min_durability` set to a different tier gets
  `INVALID_CONFIG`.

A SQL user gets the same guarantee with
`topic.create_topic('public.orders_q', min_durability => 'replicated')`.
Warning: a SQL caller that lowers `synchronous_commit` after `topic.publish`
in the same transaction loses the guarantee. The Kafka path cannot do this.

`synchronous_standby_names` must name only physical standbys. A logical
subscriber confirms the WAL position also for tables that it does not hold.
With `FIRST k`, when fewer than k connected standbys match the names, every
commit blocks.

`Metadata` lists one replica, node 0, for each band, because pg_topics is one
broker. A tool that counts those replicas shows a replication factor of 1.
The read-only topic config `pg_topics.replication_factor` gives the real
number (for example `kafka-configs --describe --all --entity-type topics
--entity-name public.orders_q`). For a `replicated` topic it is 1 + k from
the current `synchronous_standby_names`. For any other topic it is 1. So when
you change `synchronous_standby_names`, the value changes too.
`CreateTopics` checks N only when it makes the topic.

Which setting to use, with a primary and two standbys `s1` and `s2`:

- For replication factor 2, use a quorum: `ANY 1 (s1, s2)`. Commits continue
  when one standby stops.
- For replication factor 3, use `FIRST 2 (s1, s2)` or `ANY 2 (s1, s2)`.

Warning: with replication factor 3 and two required standbys, one stopped
standby blocks every commit. The commit waits with no time limit until the
standby comes back. This is true for each commit at `synchronous_commit = on`
or higher in the cluster, not only for topics. Waiting publishers hold their
backends, and enough of them use all of `pg_topics.max_clients`. Monitor
`topic.syncrep_waiters`. Read "The replicated tier" in
`docs/plans/2026-09-27-pg-topics-design.md` before you use this tier.

## Performance

These numbers come from `bench/run_benchmark.sh`, with `BENCH_SECONDS=120`, on
one machine: an AMD Ryzen 9 5900X (12 cores, 24 threads), 31 GiB RAM,
Windows WSL2, PostgreSQL 17.11, and a release build of `pg_topics`.
The Kafka clients are the Java tools from `confluentinc/cp-kafka:7.7.1`, and
they run in Docker Desktop. Each record is about 200 bytes of JSON. The
producer is idempotent and uses `acks=all`. The latency is the time from send
to acknowledgement, as `kafka-producer-perf-test` reports it.

Warning: every node runs on this one host, so replication adds no network
latency. Docker Desktop adds one network hop through Windows to each request.
Measure on your own hardware before you size a deployment.

### One node

| Test | Rate | p50 | p99 | p99.9 |
|---|---:|---:|---:|---:|
| Produce at a fixed 10,000 records/s, 4 bands | 9,997/s | 22 ms | 51 ms | 198 ms |
| Produce at a fixed 10,000 records/s, 12 bands | 9,997/s | 23 ms | 42 ms | 168 ms |
| Consume the same records, one group | 53,000/s | | | |
| Produce to consume, end to end (10 samples) | | 22 ms | 57 ms | |
| Top speed, 1 producer | 31,000/s | 2.9 s | 4.7 s | |
| Top speed, 4 producers | 61,000/s | | 2.3 s | |

The stamper gave each record its offset within 0.28 s at 10,000 records/s.
At top speed the producers queue, so the latency is in seconds. For low
latency, keep a node at about 10,000 records/s.

### Three nodes, one cluster

A primary and two synchronous streaming standbys. The topic uses the
`replicated` tier, so a Kafka client sees replication factor 3.

| `synchronous_standby_names` | Copies at ack | Rate | p50 | p99 | p99.9 |
|---|---|---:|---:|---:|---:|
| `FIRST 2 (s1, s2)` | 3 of 3 | 9,994/s | 51 ms | 142 ms | 265 ms |
| `ANY 1 (s1, s2)` | 2 of 3 | 9,996/s | 47 ms | 109 ms | 219 ms |

Consumers read at about 53,000 to 58,000 records/s from the primary in both
setups.

### SQL publish

One `topic.publish` in each transaction, from `pgbench`. Each transaction
waits for its own commit, so this is much slower than a Kafka producer, which
sends records in batches.

| Setup | 1 client | 4 clients | 8 clients |
|---|---:|---:|---:|
| One node, `durable` | 515/s | 1,193/s | 2,264/s |
| Three nodes, 3 of 3 copies | 181/s | 361/s | 670/s |
| Three nodes, 2 of 3 copies | 183/s | 363/s | 669/s |

To run the suite yourself:

```bash
BENCH_SECONDS=120 bash bench/run_benchmark.sh
```

## Monitoring

Every view below lives in schema `topic`. `SELECT` on each one, and `EXECUTE`
on `topic.error_rows()`, `topic.duplicate_offsets()` and `topic.health()`, is
granted only to the `pg_monitor` role. Any other role is refused.

| View or function | Reports |
|---|---|
| `stamp_backlog` | Each topic's last measured backlog age, and when it was last stamped. |
| `oldest_xact` | The age of the oldest open transaction in the cluster. |
| `detach_waiting` | A partition detach that retention has started, and whether it is stuck on a lock. |
| `write_partition_dead_tuples` | Dead tuples on the partition each topic is currently writing to. |
| `partition_headroom` | How long the newest partition of each topic still covers. |
| `worker_headroom` | Free slots left in `max_worker_processes`. |
| `listener_status` | What each database's Kafka listener reported at start. |
| `group_expiry` | How many members of each consumer group were expired by a missed heartbeat. |
| `error_rows()` | Rows in each topic's error table, and how many arrived in the last hour. |
| `producer_rows` | Rows held per topic for idempotent producer tracking. |
| `syncrep_waiters` | Backends stuck waiting on a synchronous standby. |
| `sync_lag` | How far each topic's sync worker is behind. |
| `consumer_lag` | How far every consumer group is behind, per topic and band. |
| `duplicate_offsets()` | A duplicate log offset on any topic — the most serious signal. |

`topic.health()` returns one row per topic, with its backlog age, its
partition headroom, whether its database's listener is bound, whether it has
a duplicate offset, whether any backend is stuck on `SyncRep`, and one `ok`
column. `ok` is false when any of those cross its threshold. Poll `health()`
from whatever already scrapes Postgres; it needs no separate exporter.

## Known limits

- `Produce` always answers `base_offset = -1`. The real offset is not known
  until the stamper runs, after the commit. A stock client still works. Java
  reports `-1` for each record. librdkafka reports `-1` for the first record
  of a batch and then counts up from there, and those later numbers are not
  real offsets.
- `Fetch` returns the record value re-serialised from `jsonb`. Field order and
  whitespace can change. A consumer that checks a signature over the raw
  bytes it sent will not match.
- `Metadata` lists one replica for each band, also for a topic made with a
  replication factor above 1. Read `pg_topics.replication_factor` for the
  real number of copies (see [Replication factor](#replication-factor)).
- Publishing to a topic means trusting its owner. A trigger the owner puts on
  the queue table runs as the publisher, the same as any Postgres trigger.
- A producer identity that sits idle for more than 7 days is reaped. The
  producer then gets `UNKNOWN_PRODUCER_ID` and must restart.
- Retention never drops a partition that still holds a row with no offset.
  Such a partition is reattached instead, and its topic's retention holds for
  one more `retention_interval`. A late-arriving row can therefore stay
  readable well past its topic's stated retention.
- A pending partition detach only finishes with `FINALIZE`, which waits for
  every older snapshot in the database to end. One long query anywhere in the
  database can stop retention on every topic until that query ends.
- A consumer group can mix a Kafka member with a SQL member only when the SQL
  member sends Kafka-shaped consumer-protocol metadata itself. A Kafka leader
  otherwise treats a SQL member as having no metadata, and gives it no bands.
- A renamed queue table leaves its control row behind. The topic then stops
  working, and another role with `CREATE` on that schema can create a new
  table with the old name.
- The Kafka wire codec (the `kafka-protocol` crate) is vendored under
  `vendor/kafka-protocol`, with a small patch against a crash on a malformed
  request. Remove the vendored copy once the fix lands upstream.
