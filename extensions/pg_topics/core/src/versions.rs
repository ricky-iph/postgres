use kafka_protocol::messages::ApiKey;

pub const VERSIONS: [(ApiKey, i16, i16); 7] = [
    (ApiKey::ApiVersions, 0, 3),
    (ApiKey::SaslHandshake, 1, 1),
    (ApiKey::SaslAuthenticate, 0, 2),
    (ApiKey::Metadata, 0, 12),
    (ApiKey::Produce, 3, 9),
    (ApiKey::Fetch, 4, 12),
    (ApiKey::ListOffsets, 1, 7),
];

pub fn supported(key: ApiKey, version: i16) -> bool {
    VERSIONS
        .iter()
        .any(|&(k, lo, hi)| k == key && (lo..=hi).contains(&version))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_advertised_version_is_in_the_crate_range_and_below_topic_ids() {
        let topic_ids = [
            (ApiKey::Metadata, 13),
            (ApiKey::Produce, 13),
            (ApiKey::Fetch, 13),
        ];
        for (key, lo, hi) in VERSIONS {
            let crate_range = key.valid_versions();
            assert!(
                crate_range.min <= lo && lo <= hi && hi <= crate_range.max,
                "{key:?} {lo}-{hi} is outside {crate_range}"
            );
            if let Some((_, cutoff)) = topic_ids.iter().find(|(k, _)| *k == key) {
                assert!(hi < *cutoff, "{key:?} {hi} uses topic IDs");
            }
        }
    }

    #[test]
    fn supported_checks_both_ends() {
        assert!(supported(ApiKey::Produce, 3));
        assert!(supported(ApiKey::Produce, 9));
        assert!(!supported(ApiKey::Produce, 2));
        assert!(!supported(ApiKey::Produce, 10));
        assert!(!supported(ApiKey::SaslHandshake, 0));
        assert!(!supported(ApiKey::JoinGroup, 0));
    }
}
