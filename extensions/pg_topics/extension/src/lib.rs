// pgrx::pg_module_magic! checks every pgrx pgNN feature; only pg14..pg17 are declared here.
#![allow(unexpected_cfgs)]

use std::ffi::CStr;
use std::panic::AssertUnwindSafe;

use pgrx::datum::Interval;
use pgrx::prelude::*;
use pgrx::spi::{self, quote_qualified_identifier};
use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};

pgrx::pg_module_magic!();

extension_sql_file!("../sql/pg_topics.sql", finalize);

static FAILOVER_IS_FENCED: GucSetting<bool> = GucSetting::<bool>::new(false);

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

#[pg_extern]
#[search_path(pg_catalog, pg_temp)]
fn stamp_topic(schema_name: &str, topic: &str, max_rows: default!(i32, 10000)) -> spi::Result<i32> {
    let names = || vec![text_arg(schema_name), text_arg(topic)];
    Spi::run("SET LOCAL row_security = off")?;
    let owner = Spi::get_one_with_args::<pg_sys::Oid>(
        "SELECT (SELECT c.relowner FROM topic.topic_config t
                 JOIN pg_catalog.pg_namespace n ON n.nspname = t.schema_name
                 JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid AND c.relname = t.topic
                 WHERE t.schema_name = $1 AND t.topic = $2)",
        names(),
    )?
    .unwrap_or_else(|| error!("topic.stamp_topic: topic {schema_name}.{topic} does not exist"));
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

    let queue = quote_qualified_identifier(schema_name, topic);
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
            "SELECT coalesce((SELECT now() - published_at FROM {queue}
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
    let mut args = names();
    args.push((PgBuiltInOids::INTERVALOID.oid(), backlog.into_datum()));
    Spi::run_with_args(
        "UPDATE topic.topic_config SET backlog_age = $3
         WHERE schema_name = $1 AND topic = $2 AND backlog_age <> $3",
        Some(args),
    )?;

    if total > 0 {
        Spi::run_with_args(
            "SELECT pg_catalog.pg_notify('pg_topics_stamped', $1 || '.' || $2)",
            Some(names()),
        )?;
    }
    Ok(total as i32)
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
