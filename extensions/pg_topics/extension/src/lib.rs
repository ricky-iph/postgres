// pgrx::pg_module_magic! checks every pgrx pgNN feature; only pg14..pg17 are declared here.
#![allow(unexpected_cfgs)]

use pgrx::prelude::*;

pgrx::pg_module_magic!();

extension_sql_file!("../sql/pg_topics.sql");

#[pg_extern(immutable, strict)]
fn band_for(key: &str, band_count: i32) -> i32 {
    if !(1..=1024).contains(&band_count) {
        error!("topic.band_for: band_count must be between 1 and 1024, got {band_count}");
    }
    pgt::murmur2::band_for(key.as_bytes(), band_count as u32) as i32
}

#[pg_guard]
pub extern "C" fn _PG_init() {
    if unsafe { !pg_sys::process_shared_preload_libraries_in_progress } {
        error!("pg_topics must be loaded via shared_preload_libraries");
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
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![
            "track_commit_timestamp = on",
            "shared_preload_libraries = 'pg_topics'",
        ]
    }
}
