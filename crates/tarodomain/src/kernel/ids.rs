//! UUID helpers: new UUIDv7s, parsing, and the deterministic UUIDv7s that saga
//! commands get.

use sha2::{Digest, Sha256};
pub use uuid::Uuid;

/// The length of a hyphenated UUID, the only text form [`parse_uuid`] accepts.
const HYPHENATED_LEN: usize = 36;

/// A new UUIDv7 for the current time.
///
/// UUIDv7s from one process sort in creation order. On
/// `wasm32-unknown-unknown` the time and the random bits come from the
/// JavaScript runtime, so this also works inside a Cloudflare Worker.
pub fn new_v7() -> Uuid {
    Uuid::now_v7()
}

/// Parses a hyphenated UUID of any version, in upper or lower case.
///
/// Returns `None` for anything else, including the other text forms (simple,
/// braced, URN), so that each ID has exactly one accepted spelling apart from
/// case. Callers attach their own error code, such as `INVALID_EVENT_ID`.
pub fn parse_uuid(text: &str) -> Option<Uuid> {
    if text.len() != HYPHENATED_LEN {
        return None;
    }
    Uuid::try_parse(text).ok()
}

/// Whether `id` is a version 7 UUID with the standard (RFC 9562) variant.
pub fn is_v7(id: &Uuid) -> bool {
    id.get_version_num() == 7 && id.get_variant() == uuid::Variant::RFC4122
}

/// A deterministic UUIDv7.
///
/// The 48-bit timestamp is `unix_ms` (milliseconds since the Unix epoch), and
/// the other 74 bits come from SHA-256 of `seed` followed by `index`. The same
/// input always gives the same ID, and it is still a valid UUIDv7.
///
/// Sagas use it for the commands they dispatch: the seed is
/// `"<saga type>/<event id>"`, the index is the command's position, and the
/// time is the event's `occurred_at`. A redelivered event then recreates the
/// same command IDs, and command idempotency skips them.
pub fn derived_v7(seed: impl AsRef<[u8]>, index: u32, unix_ms: u64) -> Uuid {
    let digest = Sha256::new()
        .chain_update(seed.as_ref())
        .chain_update(index.to_be_bytes())
        .finalize();
    let mut random = [0_u8; 10];
    random.copy_from_slice(&digest[..10]);
    uuid::Builder::from_unix_timestamp_millis(unix_ms, &random).into_uuid()
}

#[cfg(test)]
mod tests {
    use super::*;

    const V4: &str = "550e8400-e29b-41d4-a716-446655440000";

    fn unix_ms_of(id: &Uuid) -> u64 {
        let b = id.as_bytes();
        u64::from_be_bytes([0, 0, b[0], b[1], b[2], b[3], b[4], b[5]])
    }

    #[test]
    fn new_ids_are_v7_and_distinct() {
        let (a, b) = (new_v7(), new_v7());
        assert!(is_v7(&a));
        assert_ne!(a, b);
        assert!(a < b, "UUIDv7s from one process sort in creation order");
    }

    #[test]
    fn is_v7_rejects_other_versions() {
        let v4 = parse_uuid(V4).unwrap();
        assert_eq!(v4.get_version_num(), 4);
        assert!(!is_v7(&v4));
        assert!(!is_v7(&Uuid::nil()));
    }

    #[test]
    fn parsing_ignores_case() {
        let lower = parse_uuid("0190f0a2-7b3c-7d4e-8f00-123456789abc").unwrap();
        let upper = parse_uuid("0190F0A2-7B3C-7D4E-8F00-123456789ABC").unwrap();
        assert_eq!(lower, upper);
        assert_eq!(upper.to_string(), "0190f0a2-7b3c-7d4e-8f00-123456789abc");
    }

    #[test]
    fn parsing_rejects_other_forms() {
        for text in [
            "",
            "not-a-uuid",
            "550e8400e29b41d4a716446655440000",
            "{550e8400-e29b-41d4-a716-446655440000}",
            "urn:uuid:550e8400-e29b-41d4-a716-446655440000",
            "550e8400-e29b-41d4-a716-44665544000g",
        ] {
            assert_eq!(parse_uuid(text), None, "{text}");
        }
    }

    #[test]
    fn derived_ids_are_deterministic() {
        let at = 1_763_280_000_123;
        let id = derived_v7("TransferSaga/0190f0a2-7b3c-7d4e-8f00-123456789abc", 0, at);
        assert_eq!(
            id,
            derived_v7("TransferSaga/0190f0a2-7b3c-7d4e-8f00-123456789abc", 0, at)
        );
        assert!(is_v7(&id));
        assert_eq!(unix_ms_of(&id), at);
    }

    #[test]
    fn derived_ids_depend_on_every_input() {
        let at = 1_763_280_000_123;
        let id = derived_v7("saga/event", 0, at);
        assert_ne!(id, derived_v7("saga/event", 1, at));
        assert_ne!(id, derived_v7("saga/other", 0, at));
        assert_ne!(id, derived_v7("saga/event", 0, at + 1));
    }
}
