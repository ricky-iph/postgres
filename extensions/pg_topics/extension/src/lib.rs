// pgrx::pg_module_magic! checks every pgrx pgNN feature; only pg14..pg17 are declared here.
#![allow(unexpected_cfgs)]

use std::collections::HashMap;
use std::ffi::CStr;
use std::panic::{AssertUnwindSafe, UnwindSafe};
use std::time::{Duration, Instant};

use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};
use pgrx::datum::Interval;
use pgrx::pg_sys::panic::CaughtError;
use pgrx::prelude::*;
use pgrx::spi::{self, quote_qualified_identifier};
use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};

pgrx::pg_module_magic!();

extension_sql_file!("../sql/pg_topics.sql", finalize);

static FAILOVER_IS_FENCED: GucSetting<bool> = GucSetting::<bool>::new(false);
static DATABASES: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);
const STAMP_LOCK: i32 = 0x7067_7473;

#[pg_extern(immutable, strict)]
#[search_path(pg_catalog, pg_temp)]
fn band_for(key: &str, band_count: i32) -> i32 {
    if !(1..=1024).contains(&band_count) {
        error!("topic.band_for: band_count must be between 1 and 1024, got {band_count}");
    }
    pgt::murmur2::band_for(key.as_bytes(), band_count as u32) as i32
}

#[pg_extern]
#[search_path(pg_catalog, pg_temp)]
fn caller() -> String {
    if unsafe { pg_sys::InSecurityRestrictedOperation() } {
        error!("topic.caller: refused inside a security-restricted operation");
    }
    unsafe { CStr::from_ptr(pg_sys::GetUserNameFromId(pg_sys::GetOuterUserId(), false)) }
        .to_string_lossy()
        .into_owned()
}

fn as_owner<R>(owner: pg_sys::Oid, f: impl FnOnce() -> R) -> R {
    let mut saved_uid = pg_sys::InvalidOid;
    let mut saved_ctx = 0;
    unsafe {
        pg_sys::GetUserIdAndSecContext(&mut saved_uid, &mut saved_ctx);
        pg_sys::SetUserIdAndSecContext(
            owner,
            saved_ctx
                | (pg_sys::SECURITY_LOCAL_USERID_CHANGE | pg_sys::SECURITY_RESTRICTED_OPERATION)
                    as i32,
        );
    }
    PgTryBuilder::new(AssertUnwindSafe(f))
        .finally(|| unsafe { pg_sys::SetUserIdAndSecContext(saved_uid, saved_ctx) })
        .execute()
}

fn text_arg(value: &str) -> (PgOid, Option<pg_sys::Datum>) {
    (PgBuiltInOids::TEXTOID.oid(), value.into_datum())
}

fn read_one<T: FromDatum + IntoDatum>(
    sql: &str,
    args: Vec<(PgOid, Option<pg_sys::Datum>)>,
) -> spi::Result<Option<T>> {
    Spi::connect(|client| client.select(sql, Some(1), Some(args))?.first().get_one())
}

fn queue_owner(caller: &str, schema_name: &str, topic: &str) -> spi::Result<pg_sys::Oid> {
    read_one::<String>(
        "SELECT pg_catalog.set_config('row_security', 'off', true)",
        vec![],
    )?;
    let (owner, kind) = Spi::connect(|client| {
        client
            .select(
                "SELECT r.relowner, r.relkind::text FROM (VALUES (1)) v
                 LEFT JOIN LATERAL (SELECT c.relowner, c.relkind FROM topic.topic_config t
                     JOIN pg_catalog.pg_namespace n ON n.nspname = t.schema_name
                     JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid AND c.relname = t.topic
                     WHERE t.schema_name = $1 AND t.topic = $2) r ON true",
                Some(1),
                Some(vec![text_arg(schema_name), text_arg(topic)]),
            )?
            .first()
            .get_two::<pg_sys::Oid, String>()
    })?;
    match (owner, kind.as_deref()) {
        (Some(owner), Some("p")) => Ok(owner),
        (Some(_), _) => error!("{caller}: {schema_name}.{topic} is not a partitioned table"),
        _ => error!("{caller}: topic {schema_name}.{topic} does not exist"),
    }
}

#[pg_extern]
#[search_path(pg_catalog, pg_temp)]
fn stamp_topic(schema_name: &str, topic: &str, max_rows: default!(i32, 10000)) -> spi::Result<i32> {
    let names = || vec![text_arg(schema_name), text_arg(topic)];
    let owner = queue_owner("topic.stamp_topic", schema_name, topic)?;
    let queue = quote_qualified_identifier(schema_name, topic);
    let beat = |backlog: Option<Interval>| {
        let mut args = names();
        args.push((PgBuiltInOids::INTERVALOID.oid(), backlog.into_datum()));
        let stale = read_one::<bool>(
            "SELECT backlog_age IS DISTINCT FROM $3 OR stamped_at < clock_timestamp() - interval '1 second'
             FROM topic.topic_config WHERE schema_name = $1 AND topic = $2",
            args.clone(),
        )?;
        if stale != Some(true) {
            return Ok(());
        }
        Spi::run_with_args(
            "UPDATE topic.topic_config
             SET backlog_age = $3,
                 stamped_at = CASE WHEN stamped_at < clock_timestamp() - interval '1 second'
                                   THEN clock_timestamp() ELSE stamped_at END
             WHERE schema_name = $1 AND topic = $2",
            Some(args),
        )
    };
    let pending = as_owner(owner, || {
        read_one::<bool>(
            &format!("SELECT EXISTS (SELECT FROM {queue} WHERE log_offset IS NULL)"),
            vec![],
        )
    })?;
    if pending != Some(true) {
        beat(Interval::new(0, 0, 0).ok())?;
        return Ok(0);
    }
    let (next, stamped_by) = Spi::get_two_with_args::<Vec<i64>, Vec<Option<String>>>(
        "SELECT array_agg(next_offset ORDER BY band), array_agg(stamped_by ORDER BY band)
         FROM (SELECT band, next_offset, stamped_by FROM topic.topic_band_position
               WHERE schema_name = $1 AND topic = $2 FOR UPDATE) b",
        names(),
    )?;
    let node = Spi::get_one::<String>(
        "SELECT s.system_identifier || '/' || c.timeline_id
         FROM pg_catalog.pg_control_system() s, pg_catalog.pg_control_checkpoint() c",
    )?
    .unwrap_or_default();

    let (bands, counts, backlog) = as_owner(owner, || -> spi::Result<_> {
        let (bands, counts, exact) = Spi::get_three_with_args::<Vec<i16>, Vec<i64>, bool>(
            &format!(
                "WITH batch AS (
                     SELECT seq, published_at, band,
                            row_number() OVER (PARTITION BY band ORDER BY seq) - 1 AS n
                     FROM (SELECT seq, published_at, band FROM {queue}
                           WHERE log_offset IS NULL ORDER BY seq LIMIT $1) s),
                 stamped AS (
                     UPDATE {queue} q SET log_offset = ($2::bigint[])[b.band + 1] + b.n
                     FROM batch b WHERE q.published_at = b.published_at AND q.seq = b.seq
                     RETURNING q.band, q.log_offset)
                 SELECT coalesce(array_agg(band), '{{}}'), coalesce(array_agg(k), '{{}}'),
                        coalesce(sum(k), 0) = (SELECT count(*) FROM batch) AND coalesce(bool_and(exact), true)
                 FROM (SELECT band, count(*) AS k,
                              min(log_offset) = ($2::bigint[])[band + 1]
                              AND max(log_offset) = ($2::bigint[])[band + 1] + count(*) - 1
                              AND count(DISTINCT log_offset) = count(*) AS exact
                       FROM stamped GROUP BY band) c"
            ),
            vec![
                (PgBuiltInOids::INT4OID.oid(), max_rows.into_datum()),
                (PgBuiltInOids::INT8ARRAYOID.oid(), next.into_datum()),
            ],
        )?;
        if exact != Some(true) {
            error!(
                "topic.stamp_topic: {schema_name}.{topic} got offsets that do not match the batch"
            );
        }
        let backlog = Spi::get_one::<Interval>(&format!(
            "SELECT coalesce((SELECT clock_timestamp() - published_at FROM {queue}
                              WHERE log_offset IS NULL ORDER BY seq LIMIT 1), interval '0')"
        ))?;
        Ok((
            bands.unwrap_or_default(),
            counts.unwrap_or_default(),
            backlog,
        ))
    })?;

    let stamped_by = stamped_by.unwrap_or_default();
    for &band in &bands {
        if let Some(Some(old)) = stamped_by.get(band as usize) {
            if *old != node {
                warning!("topic.stamp_topic: {schema_name}.{topic} band {band} was stamped by {old}, now by {node}");
            }
        }
    }
    let total: i64 = counts.iter().sum();
    let mut args = names();
    args.push(text_arg(&node));
    args.push((PgBuiltInOids::INT2ARRAYOID.oid(), bands.into_datum()));
    args.push((PgBuiltInOids::INT8ARRAYOID.oid(), counts.into_datum()));
    Spi::run_with_args(
        "UPDATE topic.topic_band_position p
         SET next_offset = p.next_offset + u.k, stamped_by = $3
         FROM unnest($4::smallint[], $5::bigint[]) u(band, k)
         WHERE p.schema_name = $1 AND p.topic = $2 AND p.band = u.band",
        Some(args),
    )?;
    beat(backlog)?;

    if total > 0 {
        Spi::run_with_args(
            "SELECT pg_catalog.pg_notify('pg_topics_stamped', $1 || '.' || $2)",
            Some(names()),
        )?;
    }
    Ok(total as i32)
}

#[pg_extern]
#[search_path(pg_catalog, pg_temp)]
fn retention_check(detached: pg_sys::Oid) -> spi::Result<i64> {
    let (schema_name, topic) = Spi::connect(|client| {
        client
            .select(
                "SELECT schema_name, topic FROM topic.topic_config WHERE detaching = $1",
                Some(1),
                Some(vec![(PgBuiltInOids::OIDOID.oid(), detached.into_datum())]),
            )?
            .first()
            .get_two::<String, String>()
    })?;
    let (Some(schema_name), Some(topic)) = (schema_name, topic) else {
        error!(
            "topic.retention_check: {} is not a table that retention detaches",
            detached.as_u32()
        );
    };
    let owner = queue_owner("topic.retention_check", &schema_name, &topic)?;
    let table = read_one::<String>(
        "SELECT $1::pg_catalog.regclass::text",
        vec![(PgBuiltInOids::OIDOID.oid(), detached.into_datum())],
    )?
    .unwrap_or_default();
    as_owner(owner, || {
        read_one::<i64>(
            &format!("SELECT count(*) FROM {table} WHERE log_offset IS NULL"),
            vec![],
        )
    })
    .map(Option::unwrap_or_default)
}

#[pg_extern]
#[search_path(pg_catalog, pg_temp)]
fn retention_floor(
    schema_name: &str,
    topic: &str,
) -> spi::Result<TableIterator<'static, (name!(band, i16), name!(floor, i64))>> {
    let owner = queue_owner("topic.retention_floor", schema_name, topic)?;
    let (bands, next) = Spi::get_two_with_args::<Vec<i16>, Vec<i64>>(
        "SELECT array_agg(band ORDER BY band), array_agg(next_offset ORDER BY band)
         FROM (SELECT band, next_offset FROM topic.topic_band_position
               WHERE schema_name = $1 AND topic = $2 FOR UPDATE) b",
        vec![text_arg(schema_name), text_arg(topic)],
    )?;
    let (bands, next) = (bands.unwrap_or_default(), next.unwrap_or_default());
    let queue = quote_qualified_identifier(schema_name, topic);
    let lowest = as_owner(owner, || {
        read_one::<Vec<Option<i64>>>(
            &format!(
                "SELECT array_agg((SELECT min(q.log_offset) FROM {queue} q
                                   WHERE q.band = b AND q.log_offset IS NOT NULL) ORDER BY b)
                 FROM unnest($1::smallint[]) b"
            ),
            vec![(
                PgBuiltInOids::INT2ARRAYOID.oid(),
                bands.clone().into_datum(),
            )],
        )
    })?
    .unwrap_or_default();
    Ok(TableIterator::new(
        bands
            .into_iter()
            .zip(next)
            .zip(lowest)
            .map(|((band, next), low)| (band, low.unwrap_or(next))),
    ))
}

#[pg_extern]
#[search_path(pg_catalog, pg_temp)]
fn check_duplicates(
    schema_name: &str,
    topic: &str,
    full: default!(bool, false),
) -> spi::Result<
    TableIterator<'static, (name!(band, i16), name!(log_offset, i64), name!(copies, i64))>,
> {
    let owner = queue_owner("topic.check_duplicates", schema_name, topic)?;
    let interval = read_one::<Interval>(
        "SELECT partition_interval FROM topic.topic_config WHERE schema_name = $1 AND topic = $2",
        vec![text_arg(schema_name), text_arg(topic)],
    )?;
    let queue = quote_qualified_identifier(schema_name, topic);
    let sql = if full {
        format!(
            "SELECT q.band, q.log_offset, count(*) FROM {queue} q WHERE q.log_offset IS NOT NULL
             GROUP BY q.band, q.log_offset HAVING count(*) > 1 ORDER BY 1, 2"
        )
    } else {
        format!(
            "SELECT DISTINCT d.band, d.log_offset, d.copies FROM (
                 SELECT r.band, r.log_offset,
                        (SELECT count(*) FROM {queue} q WHERE q.band = r.band AND q.log_offset = r.log_offset) AS copies
                 FROM {queue} r
                 WHERE r.log_offset IS NOT NULL
                   AND r.published_at >= pg_catalog.date_bin($1, pg_catalog.now(), timestamptz '2000-01-01 00:00:00+00')) d
             WHERE d.copies > 1 ORDER BY 1, 2"
        )
    };
    let rows = as_owner(owner, || {
        Spi::connect(|client| {
            client
                .select(
                    &sql,
                    None,
                    Some(vec![(
                        PgBuiltInOids::INTERVALOID.oid(),
                        interval.into_datum(),
                    )]),
                )?
                .map(|row| {
                    Ok((
                        row.get::<i16>(1)?.unwrap_or_default(),
                        row.get::<i64>(2)?.unwrap_or_default(),
                        row.get::<i64>(3)?.unwrap_or_default(),
                    ))
                })
                .collect::<spi::Result<Vec<_>>>()
        })
    })?;
    Ok(TableIterator::new(rows))
}

fn database_names(list: &str) -> Vec<&str> {
    let mut names = Vec::new();
    for name in list.split(',').map(str::trim) {
        if !name.is_empty() && !names.contains(&name) {
            names.push(name);
        }
    }
    names
}

fn in_transaction<R>(f: impl FnOnce() -> spi::Result<R> + UnwindSafe) -> R {
    BackgroundWorker::transaction(AssertUnwindSafe(|| f().unwrap_or_else(|e| error!("{e}"))))
}

fn guarded<R>(f: impl FnOnce() -> R + UnwindSafe) -> Option<R> {
    PgTryBuilder::new(|| Some(f()))
        .catch_others(|e| {
            unsafe { pg_sys::AbortCurrentTransaction() };
            let (CaughtError::PostgresError(report)
            | CaughtError::ErrorReport(report)
            | CaughtError::RustPanic {
                ereport: report, ..
            }) = e;
            warning!(
                "pg_topics stamper: {}. The stamper tries again in 1 s.",
                report.message()
            );
            None
        })
        .execute()
}

fn topic_list() -> spi::Result<Option<Vec<(String, String)>>> {
    if read_one::<bool>(
        "SELECT EXISTS (SELECT FROM pg_catalog.pg_extension WHERE extname = 'pg_topics')",
        vec![],
    )? != Some(true)
    {
        return Ok(None);
    }
    let (schemas, topics) = Spi::connect(|client| {
        client
            .select(
                "SELECT array_agg(schema_name ORDER BY schema_name, topic),
                        array_agg(topic ORDER BY schema_name, topic)
                 FROM topic.topic_config",
                Some(1),
                None,
            )?
            .first()
            .get_two::<Vec<String>, Vec<String>>()
    })?;
    Ok(Some(
        schemas
            .unwrap_or_default()
            .into_iter()
            .zip(topics.unwrap_or_default())
            .collect(),
    ))
}

fn stamp_locked(schema_name: &str, topic: &str) -> spi::Result<i32> {
    let args = || {
        vec![
            text_arg(schema_name),
            text_arg(topic),
            (PgBuiltInOids::INT4OID.oid(), STAMP_LOCK.into_datum()),
        ]
    };
    if read_one::<bool>(
        "SELECT pg_catalog.pg_try_advisory_xact_lock($3, pg_catalog.hashtext($1 || '.' || $2))",
        args(),
    )? != Some(true)
    {
        return Ok(0);
    }
    Ok(read_one::<i32>("SELECT topic.stamp_topic($1, $2)", args())?.unwrap_or(0))
}

fn wait_latch(ms: i64) -> bool {
    unsafe {
        pg_sys::WaitLatch(
            pg_sys::MyLatch,
            (pg_sys::WL_LATCH_SET | pg_sys::WL_TIMEOUT | pg_sys::WL_EXIT_ON_PM_DEATH) as i32,
            ms,
            pg_sys::PG_WAIT_EXTENSION,
        );
        pg_sys::ResetLatch(pg_sys::MyLatch);
        pg_sys::check_for_interrupts!();
    }
    !BackgroundWorker::sigterm_received()
}

#[no_mangle]
#[pg_guard]
pub extern "C" fn pg_topics_stamper_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGTERM);
    BackgroundWorker::connect_worker_to_spi(Some(BackgroundWorker::get_extra()), None);
    in_transaction(|| Spi::run("SET search_path = pg_catalog, pg_temp"));
    let mut retry_after: HashMap<(String, String), Instant> = HashMap::new();
    let mut pause = 50;
    while wait_latch(pause) {
        let Some(Some(topics)) = guarded(|| in_transaction(topic_list)) else {
            pause = 1000;
            continue;
        };
        let mut stamped = false;
        for (schema_name, topic) in topics {
            if retry_after
                .get(&(schema_name.clone(), topic.clone()))
                .is_some_and(|at| Instant::now() < *at)
            {
                continue;
            }
            match guarded(|| in_transaction(|| stamp_locked(&schema_name, &topic))) {
                Some(n) => {
                    stamped |= n > 0;
                    retry_after.remove(&(schema_name, topic));
                }
                None => {
                    retry_after.insert(
                        (schema_name, topic),
                        Instant::now() + Duration::from_secs(1),
                    );
                }
            }
        }
        unsafe { pg_sys::pgstat_report_stat(false) };
        pause = if stamped { 1 } else { 50 };
    }
    unsafe { pg_sys::proc_exit(1) }
}

#[no_mangle]
#[pg_guard]
pub extern "C" fn pg_topics_partition_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGTERM);
    let database = BackgroundWorker::get_extra();
    BackgroundWorker::connect_worker_to_spi(Some(database), None);
    let user = in_transaction(|| {
        read_one::<String>(
            "SELECT rolname::text FROM pg_catalog.pg_authid WHERE oid = 10",
            vec![],
        )
    })
    .unwrap_or_default();
    let sockets = unsafe { CStr::from_ptr(pg_sys::Unix_socket_directories) }.to_string_lossy();
    let socket = sockets.split(',').next().unwrap_or_default().trim();
    let port = unsafe { pg_sys::PostPortNumber } as u16;
    let mut client = match pgt::partitions::connect(socket, port, &user, database) {
        Ok(client) => client,
        Err(e) => {
            warning!(
                "pg_topics partition worker: cannot connect to {database} over the socket in {socket:?}: {}. The worker tries again in 5 s.",
                pgt::partitions::message(&e)
            );
            unsafe { pg_sys::proc_exit(1) }
        }
    };
    let mut ticks: u64 = 0;
    loop {
        let result = pgt::partitions::tick(&mut client, ticks.is_multiple_of(360), &mut |m| {
            warning!("pg_topics partition worker: {m}")
        });
        if let Err(e) = result {
            warning!(
                "pg_topics partition worker: {}. The worker tries again in 5 s.",
                pgt::partitions::message(&e)
            );
            break;
        }
        ticks += 1;
        unsafe { pg_sys::pgstat_report_stat(false) };
        if !wait_latch(10_000) {
            break;
        }
    }
    unsafe { pg_sys::proc_exit(1) }
}

#[pg_guard]
pub extern "C" fn _PG_init() {
    if unsafe { !pg_sys::process_shared_preload_libraries_in_progress } {
        error!("pg_topics must be loaded via shared_preload_libraries");
    }
    GucRegistry::define_bool_guc(
        "pg_topics.failover_is_fenced",
        "The operator has fenced the old primary on failover.",
        "create_topic refuses durable and replicated topics while this is off.",
        &FAILOVER_IS_FENCED,
        GucContext::Suset,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        "pg_topics.databases",
        "The databases that get the pg_topics workers.",
        "A comma list of database names. A change needs a server restart.",
        &DATABASES,
        GucContext::Postmaster,
        GucFlags::default(),
    );
    let list = DATABASES
        .get()
        .map(|list| list.to_string_lossy().into_owned())
        .unwrap_or_default();
    for database in database_names(&list) {
        BackgroundWorkerBuilder::new(&format!("pg_topics stamper {database}"))
            .set_type("pg_topics stamper")
            .set_library("pg_topics")
            .set_function("pg_topics_stamper_main")
            .set_extra(database)
            .set_restart_time(Some(Duration::from_secs(5)))
            .enable_spi_access()
            .load();
        BackgroundWorkerBuilder::new(&format!("pg_topics partition {database}"))
            .set_type("pg_topics partition")
            .set_library("pg_topics")
            .set_function("pg_topics_partition_main")
            .set_extra(database)
            .set_restart_time(Some(Duration::from_secs(5)))
            .enable_spi_access()
            .load();
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn band_for_matches_golden_sample() {
        assert_eq!(
            Spi::get_one::<i32>("SELECT topic.band_for('a', 7)").unwrap(),
            Some(5)
        );
        assert_eq!(
            Spi::get_one::<i32>("SELECT topic.band_for('bottle-1', 4)").unwrap(),
            Some(2)
        );
        assert_eq!(
            Spi::get_one::<i32>("SELECT topic.band_for('Zürich', 1024)").unwrap(),
            Some(49)
        );
        assert_eq!(
            Spi::get_one::<i32>("SELECT topic.band_for('θάλασσα', 16)").unwrap(),
            Some(2)
        );
        assert_eq!(
            Spi::get_one::<i32>("SELECT topic.band_for('मुंबई', 7)").unwrap(),
            Some(4)
        );
    }

    #[pg_test]
    fn band_for_rejects_bad_count() {
        let too_low =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| crate::band_for("x", 0)));
        assert!(too_low.is_err());

        let too_high =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| crate::band_for("x", 1025)));
        assert!(too_high.is_err());
    }

    #[pg_test]
    fn databases_guc_parses() {
        assert_eq!(
            crate::database_names(" postgres, app ,, other_db, app"),
            vec!["postgres", "app", "other_db"]
        );
        assert!(crate::database_names(" , ").is_empty());
    }

    fn one<T: IntoDatum + FromDatum>(sql: &str) -> Option<T> {
        Spi::get_one::<T>(sql).unwrap()
    }

    fn error_of(sql: &str) -> Option<String> {
        Spi::run(
            "DO $do$ BEGIN
             IF to_regprocedure('pg_temp.error_of(text)') IS NULL THEN
                 CREATE FUNCTION pg_temp.error_of(q text) RETURNS text LANGUAGE plpgsql AS $$
                 BEGIN EXECUTE q; RETURN NULL; EXCEPTION WHEN OTHERS THEN RETURN SQLERRM; END $$;
             END IF;
             END $do$",
        )
        .unwrap();
        Spi::get_one_with_args::<String>(
            "SELECT pg_temp.error_of($1)",
            vec![(PgBuiltInOids::TEXTOID.oid(), sql.into_datum())],
        )
        .unwrap()
    }

    fn tenant(role: &str) {
        Spi::run(&format!(
            "CREATE ROLE {role}; CREATE SCHEMA {role} AUTHORIZATION {role}"
        ))
        .unwrap();
    }

    #[pg_test]
    fn create_topic_builds_objects() {
        Spi::run("SELECT topic.create_topic('public.bottles_q', 4)").unwrap();
        assert_eq!(
            one::<String>(
                "SELECT relkind::text FROM pg_class WHERE oid = 'public.bottles_q'::regclass"
            ),
            Some("p".into())
        );
        assert_eq!(
            one::<i64>(
                "SELECT count(*) FROM pg_inherits WHERE inhparent = 'public.bottles_q'::regclass"
            ),
            Some(2)
        );
        assert_eq!(
            one::<i64>(
                "SELECT count(*) FROM pg_index i JOIN pg_inherits h ON h.inhrelid = i.indrelid
                 WHERE h.inhparent = 'public.bottles_q'::regclass AND i.indisunique AND NOT i.indisprimary"
            ),
            Some(2)
        );
        assert_eq!(
            one::<i64>(
                "SELECT count(*) FROM topic.topic_band_position
                 WHERE schema_name = 'public' AND topic = 'bottles_q'"
            ),
            Some(4)
        );
        assert_eq!(
            one::<String>(
                "SELECT pg_get_constraintdef(oid) FROM pg_constraint
                 WHERE conrelid = 'public.bottles_q'::regclass AND contype = 'c'"
            ),
            Some("CHECK (((band >= 0) AND (band <= 3)))".into())
        );
    }

    #[pg_test]
    fn create_topic_refuses_bad_names() {
        let long = format!("public.{}_q", "x".repeat(46));
        for call in [
            "SELECT topic.create_topic('a.b.c_q')".to_string(),
            "SELECT topic.create_topic('public.bottles')".to_string(),
            format!("SELECT topic.create_topic('{long}')"),
            "SELECT topic.create_topic('public.zero_q', 0)".to_string(),
            "SELECT topic.create_topic('public.many_q', 1025)".to_string(),
            "SELECT topic.create_topic('public.\"x;drop\"')".to_string(),
        ] {
            assert!(error_of(&call).is_some(), "not refused: {call}");
        }
        tenant("pgt_no_create");
        Spi::run("SET LOCAL ROLE pgt_no_create").unwrap();
        let refused = error_of("SELECT topic.create_topic('public.theirs_q')");
        Spi::run("RESET ROLE").unwrap();
        assert!(refused.unwrap().contains("no CREATE privilege"));
        assert_eq!(
            one::<i64>("SELECT count(*) FROM topic.topic_config"),
            Some(0)
        );
        assert_eq!(
            one::<i64>("SELECT count(*) FROM pg_class WHERE relname LIKE '%\\_q%' AND relnamespace = 'public'::regnamespace"),
            Some(0)
        );
    }

    #[pg_test]
    fn create_topic_refuses_unfenced_durability() {
        assert!(error_of(
            "SELECT topic.create_topic('public.rep_q', 1, min_durability => 'replicated')"
        )
        .unwrap()
        .contains("synchronous_standby_names"));
        Spi::run("SET LOCAL pg_topics.failover_is_fenced = off").unwrap();
        assert!(error_of("SELECT topic.create_topic('public.dur_q', 1)")
            .unwrap()
            .contains("failover_is_fenced"));
        Spi::run("SELECT topic.create_topic('public.rel_q', 1, min_durability => 'relaxed')")
            .unwrap();
    }

    #[pg_test]
    fn stamp_is_gap_free_per_band() {
        Spi::run("SELECT topic.create_topic('public.gap_q', 2)").unwrap();
        Spi::run("SELECT topic.publish('public.gap_q', jsonb_build_object('i', i)) FROM generate_series(1, 10) i").unwrap();
        assert_eq!(
            one::<i32>("SELECT topic.stamp_topic('public', 'gap_q')"),
            Some(10)
        );
        for band in 0..2 {
            assert_eq!(
                one::<Vec<i64>>(&format!(
                    "SELECT array_agg(log_offset ORDER BY log_offset) FROM public.gap_q WHERE band = {band}"
                )),
                Some(vec![0, 1, 2, 3, 4])
            );
            assert_eq!(
                one::<i64>(&format!(
                    "SELECT next_offset FROM topic.topic_band_position
                     WHERE schema_name = 'public' AND topic = 'gap_q' AND band = {band}"
                )),
                Some(5)
            );
        }
        assert_eq!(
            one::<i32>("SELECT topic.stamp_topic('public', 'gap_q')"),
            Some(0)
        );
    }

    #[pg_test]
    fn raw_insert_cannot_forge_offset() {
        tenant("pgt_forger");
        Spi::run("SET LOCAL ROLE pgt_forger").unwrap();
        Spi::run("SELECT topic.create_topic('pgt_forger.forge_q', 1)").unwrap();
        let offset = error_of("INSERT INTO pgt_forger.forge_q (band, log_offset) VALUES (0, 7)");
        let author =
            error_of("INSERT INTO pgt_forger.forge_q (band, published_by) VALUES (0, 'someone')");
        let plain = error_of("INSERT INTO pgt_forger.forge_q (band) VALUES (0)");
        Spi::run("RESET ROLE").unwrap();
        assert!(offset.unwrap().contains("must not set log_offset"));
        assert!(author.unwrap().contains("must not set log_offset"));
        assert_eq!(plain, None);
    }

    #[pg_test]
    fn publish_refuses_on_backlog() {
        Spi::run("SELECT topic.create_topic('public.slow_q', 1)").unwrap();
        Spi::run("UPDATE topic.topic_config SET backlog_age = '2 minutes' WHERE topic = 'slow_q'")
            .unwrap();
        assert!(error_of("SELECT topic.publish('public.slow_q', '{}')")
            .unwrap()
            .contains("above max_backlog_age"));
    }

    #[pg_test]
    fn publish_refuses_without_recent_stamp() {
        Spi::run("SELECT topic.create_topic('public.stale_q', 1)").unwrap();
        Spi::run("SELECT topic.publish('public.stale_q', '{}')").unwrap();
        Spi::run(
            "UPDATE topic.topic_config SET stamped_at = now() - interval '2 minutes' WHERE topic = 'stale_q'",
        )
        .unwrap();
        assert!(error_of("SELECT topic.publish('public.stale_q', '{}')")
            .unwrap()
            .contains("the stamper has not run"));
        assert_eq!(
            one::<i32>("SELECT topic.stamp_topic('public', 'stale_q')"),
            Some(1)
        );
        Spi::run("SELECT topic.publish('public.stale_q', '{}')").unwrap();
    }

    #[pg_test]
    fn max_backlog_age_is_at_least_2_seconds() {
        Spi::run("SELECT topic.create_topic('public.short_q', 1)").unwrap();
        assert!(error_of(
            "UPDATE topic.topic_config SET max_backlog_age = '1 second' WHERE topic = 'short_q'"
        )
        .unwrap()
        .contains("check constraint"));
        Spi::run(
            "UPDATE topic.topic_config SET max_backlog_age = '2 seconds' WHERE topic = 'short_q'",
        )
        .unwrap();
    }

    #[pg_test]
    fn publish_raises_synchronous_commit() {
        Spi::run("SELECT topic.create_topic('public.floor_q', 1)").unwrap();
        Spi::run("SET LOCAL synchronous_commit = off").unwrap();
        Spi::run("SELECT topic.publish('public.floor_q', '{}')").unwrap();
        assert_eq!(one::<String>("SHOW synchronous_commit"), Some("on".into()));
    }

    #[pg_test]
    fn caller_sees_set_role_inside_definer() {
        Spi::run("CREATE ROLE pgt_caller").unwrap();
        Spi::run(
            "CREATE FUNCTION public.pgt_who() RETURNS text SECURITY DEFINER LANGUAGE sql
             AS $$ SELECT topic.caller() || '/' || current_user $$",
        )
        .unwrap();
        let me = one::<String>("SELECT current_user::text").unwrap();
        Spi::run("SET LOCAL ROLE pgt_caller").unwrap();
        let seen = one::<String>("SELECT public.pgt_who()");
        Spi::run("RESET ROLE").unwrap();
        assert_eq!(seen, Some(format!("pgt_caller/{me}")));
    }

    #[pg_test]
    fn publish_uses_kafka_partitioner() {
        Spi::run("SELECT topic.create_topic('public.keyed_q', 4)").unwrap();
        Spi::run("SELECT topic.publish('public.keyed_q', '{}', 'bottle-1')").unwrap();
        assert_eq!(one::<i16>("SELECT band FROM public.keyed_q"), Some(2));
    }

    #[pg_test]
    fn stamp_runs_tenant_code_as_owner() {
        tenant("pgt_owner");
        Spi::run("SET LOCAL ROLE pgt_owner").unwrap();
        Spi::run(
            "SELECT topic.create_topic('pgt_owner.own_q', 1);
             CREATE TABLE pgt_owner.seen (who name);
             CREATE FUNCTION pgt_owner.record() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN INSERT INTO pgt_owner.seen VALUES (current_user); RETURN NEW; END $$;
             CREATE TRIGGER record BEFORE UPDATE ON pgt_owner.own_q FOR EACH ROW EXECUTE FUNCTION pgt_owner.record();
             SELECT topic.publish('pgt_owner.own_q', '{}');",
        )
        .unwrap();
        Spi::run("RESET ROLE").unwrap();
        assert_eq!(
            one::<bool>(
                "SELECT has_function_privilege('pgt_owner', 'topic.stamp_topic(text, text, int)', 'EXECUTE')"
            ),
            Some(false)
        );
        assert_eq!(
            one::<i32>("SELECT topic.stamp_topic('pgt_owner', 'own_q')"),
            Some(1)
        );
        assert_eq!(
            one::<Vec<String>>("SELECT array_agg(who::text) FROM pgt_owner.seen"),
            Some(vec!["pgt_owner".into()])
        );

        Spi::run(
            "CREATE FUNCTION pgt_owner.escape() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN RESET ROLE; RETURN NEW; END $$;
             CREATE TRIGGER escape BEFORE UPDATE ON pgt_owner.own_q FOR EACH ROW EXECUTE FUNCTION pgt_owner.escape();
             SET LOCAL ROLE pgt_owner;
             SELECT topic.publish('pgt_owner.own_q', '{}');
             RESET ROLE;",
        )
        .unwrap();
        let escaped = error_of("SELECT topic.stamp_topic('pgt_owner', 'own_q')");
        assert!(
            escaped
                .as_deref()
                .is_some_and(|e| e.contains("cannot set parameter \"role\"")),
            "{escaped:?}"
        );
        assert_eq!(
            one::<i64>("SELECT count(*) FROM pgt_owner.own_q WHERE log_offset IS NULL"),
            Some(1)
        );
    }

    #[pg_test]
    fn stamp_blocks_tenant_create_topic() {
        tenant("pgt_evil");
        Spi::run("SET LOCAL ROLE pgt_evil").unwrap();
        Spi::run(
            "SELECT topic.create_topic('pgt_evil.e_q', 1);
             CREATE FUNCTION pgt_evil.squat() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN PERFORM topic.create_topic('public.squat_q', 1); RETURN NEW; END $$;
             CREATE TRIGGER squat BEFORE UPDATE ON pgt_evil.e_q FOR EACH ROW EXECUTE FUNCTION pgt_evil.squat();
             SELECT topic.publish('pgt_evil.e_q', '{}');",
        )
        .unwrap();
        Spi::run("RESET ROLE").unwrap();
        let refused = error_of("SELECT topic.stamp_topic('pgt_evil', 'e_q')");
        assert!(
            refused
                .as_deref()
                .is_some_and(|e| e.contains("security-restricted operation")),
            "{refused:?}"
        );
        assert_eq!(
            one::<bool>("SELECT to_regclass('public.squat_q') IS NULL"),
            Some(true)
        );
        assert_eq!(
            one::<i64>("SELECT count(*) FROM pgt_evil.e_q WHERE log_offset IS NULL"),
            Some(1)
        );
    }

    #[pg_test]
    fn stamp_refuses_skipped_row() {
        tenant("pgt_skip");
        Spi::run("SET LOCAL ROLE pgt_skip").unwrap();
        Spi::run(
            r#"SELECT topic.create_topic('pgt_skip.s_q', 1);
             CREATE FUNCTION pgt_skip.skip() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN IF OLD.value ? 'skip' THEN RETURN NULL; END IF; RETURN NEW; END $$;
             CREATE TRIGGER skip BEFORE UPDATE ON pgt_skip.s_q FOR EACH ROW EXECUTE FUNCTION pgt_skip.skip();
             SELECT topic.publish('pgt_skip.s_q', '{}');
             SELECT topic.publish('pgt_skip.s_q', '{"skip": 1}');
             SELECT topic.publish('pgt_skip.s_q', '{}');"#,
        )
        .unwrap();
        Spi::run("RESET ROLE").unwrap();
        let refused = error_of("SELECT topic.stamp_topic('pgt_skip', 's_q')");
        assert!(
            refused.as_deref().is_some_and(|e| e.contains("offsets")),
            "{refused:?}"
        );
        assert_eq!(
            one::<i64>("SELECT count(*) FROM pgt_skip.s_q WHERE log_offset IS NULL"),
            Some(3)
        );
    }

    #[pg_test]
    fn create_topic_bounds_ignore_session_datestyle() {
        Spi::run("SET LOCAL DateStyle = 'SQL, MDY'; SET LOCAL TimeZone = 'Asia/Kolkata'").unwrap();
        Spi::run("SELECT topic.create_topic('public.tz_q', 1)").unwrap();
        Spi::run("SET LOCAL DateStyle = 'ISO'; SET LOCAL TimeZone = 'UTC'").unwrap();
        assert_eq!(
            one::<i64>(
                "SELECT count(*) FROM pg_inherits h JOIN pg_class c ON c.oid = h.inhrelid
                 WHERE h.inhparent = 'public.tz_q'::regclass
                 AND pg_get_expr(c.relpartbound, c.oid) LIKE format('FOR VALUES FROM (%L)%%',
                     to_char(to_timestamp(right(c.relname, 14), 'YYYYMMDDHH24MISS'), 'YYYY-MM-DD HH24:MI:SS+00'))"
            ),
            Some(2)
        );
    }

    #[pg_test]
    fn every_function_pins_search_path() {
        assert_eq!(
            one::<Vec<String>>(
                "SELECT coalesce(array_agg(proname::text), '{}') FROM pg_proc
                 WHERE pronamespace = 'topic'::regnamespace
                 AND NOT coalesce('search_path=pg_catalog, pg_temp' = ANY (proconfig), false)"
            ),
            Some(vec![])
        );
    }

    #[pg_test]
    fn ensure_partitions_adds_owned_indexed_partitions() {
        tenant("pgt_parts");
        Spi::run("SET LOCAL ROLE pgt_parts").unwrap();
        Spi::run("SELECT topic.create_topic('pgt_parts.p_q', 1, partition_interval => '1 hour')")
            .unwrap();
        Spi::run("RESET ROLE").unwrap();
        Spi::run("SELECT topic.ensure_partitions('pgt_parts', 'p_q', 3)").unwrap();
        Spi::run("SELECT topic.ensure_partitions('pgt_parts', 'p_q', 3)").unwrap();
        Spi::run("SET LOCAL TimeZone = 'UTC'").unwrap();
        let parts = "FROM pg_inherits h JOIN pg_class c ON c.oid = h.inhrelid,
                     LATERAL (SELECT to_timestamp(right(c.relname, 14), 'YYYYMMDDHH24MISS') AS lo) b
                     WHERE h.inhparent = 'pgt_parts.p_q'::regclass";
        assert_eq!(
            one::<Vec<i32>>(&format!(
                "SELECT array_agg((extract(epoch FROM b.lo - date_bin('1 hour', now(), '2000-01-01')) / 3600)::int
                                  ORDER BY b.lo) {parts}"
            )),
            Some(vec![0, 1, 2, 3])
        );
        assert_eq!(
            one::<Vec<String>>(&format!(
                "SELECT array_agg(format('%s %s %s',
                    pg_get_expr(c.relpartbound, c.oid) = format('FOR VALUES FROM (%L) TO (%L)',
                        to_char(b.lo, 'YYYY-MM-DD HH24:MI:SS+00'),
                        to_char(b.lo + interval '1 hour', 'YYYY-MM-DD HH24:MI:SS+00')),
                    pg_get_userbyid(c.relowner),
                    EXISTS (SELECT FROM pg_index x WHERE x.indrelid = c.oid AND x.indisunique)))
                 {parts}"
            )),
            Some(vec!["t pgt_parts t".to_string(); 4])
        );
    }

    #[pg_test]
    fn ensure_partitions_skips_a_table_that_has_a_partition_name() {
        Spi::run("SELECT topic.create_topic('public.clash_q', 1, partition_interval => '1 hour')")
            .unwrap();
        Spi::run(
            "SET LOCAL TimeZone = 'UTC';
             CREATE TABLE public.foreign_t ();
             DO $$ BEGIN EXECUTE format('ALTER TABLE public.foreign_t RENAME TO %I',
                 'clash_q_p' || to_char(date_bin('1 hour', now(), '2000-01-01') + interval '2 hours', 'YYYYMMDDHH24MISS'));
             END $$",
        )
        .unwrap();
        Spi::run("SELECT topic.ensure_partitions('public', 'clash_q', 3)").unwrap();
        assert_eq!(
            one::<Vec<i32>>(
                "SELECT array_agg((extract(epoch FROM to_timestamp(right(c.relname, 14), 'YYYYMMDDHH24MISS')
                                   - date_bin('1 hour', now(), '2000-01-01')) / 3600)::int ORDER BY c.relname)
                 FROM pg_inherits h JOIN pg_class c ON c.oid = h.inhrelid
                 WHERE h.inhparent = 'public.clash_q'::regclass"
            ),
            Some(vec![0, 1, 3])
        );
    }

    #[pg_test]
    fn queue_functions_refuse_a_view_in_place_of_the_queue() {
        tenant("pgt_view");
        Spi::run("SET LOCAL ROLE pgt_view").unwrap();
        Spi::run(
            "SELECT topic.create_topic('pgt_view.v_q', 1, partition_interval => '1 hour');
             DROP TABLE pgt_view.v_q;
             CREATE VIEW pgt_view.v_q AS
                 SELECT 0::smallint AS band, 0::bigint AS log_offset, now() AS published_at, 0::bigint AS seq",
        )
        .unwrap();
        Spi::run("RESET ROLE").unwrap();
        for call in [
            "SELECT topic.stamp_topic('pgt_view', 'v_q')",
            "SELECT count(*) FROM topic.check_duplicates('pgt_view', 'v_q', full => true)",
            "SELECT count(*) FROM topic.retention_floor('pgt_view', 'v_q')",
            "SELECT topic.ensure_partitions('pgt_view', 'v_q', 3)",
        ] {
            let refused = error_of(call);
            assert!(
                refused
                    .as_deref()
                    .is_some_and(|e| e.contains("is not a partitioned table")),
                "{call}: {refused:?}"
            );
        }
    }

    #[pg_test]
    fn check_duplicates_finds_cross_partition_copy() {
        Spi::run("SELECT topic.create_topic('public.dup_q', 1, partition_interval => '1 hour')")
            .unwrap();
        Spi::run("SELECT topic.publish('public.dup_q', '{}') FROM generate_series(1, 3)").unwrap();
        assert_eq!(
            one::<i32>("SELECT topic.stamp_topic('public', 'dup_q')"),
            Some(3)
        );
        let found = |full: bool| {
            one::<Vec<String>>(&format!(
                "SELECT coalesce(array_agg(band || ':' || log_offset || ':' || copies), '{{}}')
                 FROM topic.check_duplicates('public', 'dup_q', full => {full})"
            ))
        };
        assert_eq!(found(true), Some(vec![]));
        assert_eq!(found(false), Some(vec![]));
        Spi::run(
            "INSERT INTO public.dup_q (band, value, published_at)
             VALUES (0, '{}', date_bin('1 hour', now(), '2000-01-01') + interval '1 hour');
             UPDATE public.dup_q SET log_offset = 1 WHERE log_offset IS NULL",
        )
        .unwrap();
        assert_eq!(
            one::<i64>("SELECT count(DISTINCT tableoid) FROM public.dup_q WHERE log_offset = 1"),
            Some(2)
        );
        assert_eq!(found(true), Some(vec!["0:1:2".to_string()]));
        assert_eq!(found(false), Some(vec!["0:1:2".to_string()]));
    }

    #[pg_test]
    fn reap_removes_old_producers_and_expired_groups() {
        Spi::run(
            "SELECT topic.create_topic('public.kept_q', 1, partition_interval => '1 hour');
             SELECT topic.create_topic('public.reaped_q', 1, partition_interval => '1 hour');
             UPDATE topic.topic_config SET offset_retention = '1 hour' WHERE topic = 'reaped_q';
             INSERT INTO topic.topic_producers
                 (schema_name, topic, producer_id, producer_epoch, band, slot, first_sequence, last_sequence, base_offset, updated_at)
             VALUES ('public', 'kept_q', 1, 0, 0, 0, 0, 0, -1, now() - interval '25 hours'),
                    ('public', 'kept_q', 2, 0, 0, 0, 0, 0, -1, now() - interval '23 hours');
             INSERT INTO topic.topic_groups (group_name, owner_role, state, updated_at)
             VALUES ('old_empty', 'postgres', 'Empty', now() - interval '2 hours'),
                    ('new_empty', 'postgres', 'Empty', now() - interval '30 minutes'),
                    ('old_stable', 'postgres', 'Stable', now() - interval '2 hours'),
                    ('old_member', 'postgres', 'Empty', now() - interval '2 hours'),
                    ('old_forever', 'postgres', 'Empty', now() - interval '2 hours'),
                    ('old_no_offsets', 'postgres', 'Empty', now() - interval '2 hours'),
                    ('__pg_topics_sync:public.reaped_q', 'postgres', 'Empty', now() - interval '2 hours');
             INSERT INTO topic.topic_group_members (group_name, member_id, owner_role, session_timeout_ms, rebalance_ms)
             VALUES ('old_member', 'm', 'postgres', 6000, 6000);
             INSERT INTO topic.topic_offsets (schema_name, topic, group_name, band, owner_role)
             SELECT 'public', 'reaped_q', g, 0, 'postgres'
             FROM unnest(ARRAY['old_empty', 'new_empty', 'old_stable', 'old_member', 'old_forever',
                               '__pg_topics_sync:public.reaped_q']) g;
             INSERT INTO topic.topic_offsets (schema_name, topic, group_name, band, owner_role)
             VALUES ('public', 'kept_q', 'old_forever', 0, 'postgres');
             SELECT topic.reap();",
        )
        .unwrap();
        assert_eq!(
            one::<Vec<i64>>(
                "SELECT array_agg(producer_id ORDER BY producer_id) FROM topic.topic_producers"
            ),
            Some(vec![2])
        );
        assert_eq!(
            one::<Vec<String>>(
                "SELECT array_agg(group_name ORDER BY group_name COLLATE \"C\") FROM topic.topic_groups"
            ),
            Some(
                [
                    "__pg_topics_sync:public.reaped_q",
                    "new_empty",
                    "old_forever",
                    "old_member",
                    "old_no_offsets",
                    "old_stable"
                ]
                .map(String::from)
                .to_vec()
            )
        );
    }

    #[pg_test]
    fn stamp_refuses_forced_rls() {
        tenant("pgt_rls");
        Spi::run("SET LOCAL ROLE pgt_rls").unwrap();
        Spi::run(
            "SELECT topic.create_topic('pgt_rls.hidden_q', 1);
             SELECT topic.publish('pgt_rls.hidden_q', '{}') FROM generate_series(1, 3);
             ALTER TABLE pgt_rls.hidden_q ENABLE ROW LEVEL SECURITY;
             ALTER TABLE pgt_rls.hidden_q FORCE ROW LEVEL SECURITY;
             CREATE POLICY hide ON pgt_rls.hidden_q USING (false);",
        )
        .unwrap();
        Spi::run("RESET ROLE").unwrap();
        assert!(error_of("SELECT topic.stamp_topic('pgt_rls', 'hidden_q')")
            .unwrap()
            .contains("row-level security"));
        assert_eq!(
            one::<Vec<i64>>("SELECT ARRAY[count(*), count(log_offset)] FROM pgt_rls.hidden_q"),
            Some(vec![3, 0])
        );
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![
            "shared_preload_libraries = 'pg_topics'",
            "pg_topics.failover_is_fenced = on",
        ]
    }
}
