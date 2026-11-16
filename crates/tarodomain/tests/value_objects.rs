//! Value objects and aggregate IDs (spec §2.3, §2.4), used from outside the
//! crate so that the `aggregate_id!` macro's paths are checked too.

use std::collections::HashSet;
use std::error::Error as _;
use std::num::ParseIntError;

use serde_json::json;
use tarodomain::aggregate_id;
use tarodomain::domain::{AggregateId, ValueObject};
use tarodomain::error::{BoxError, ErrorCategory, ErrorCode, FrameworkError};

const V4: &str = "550e8400-e29b-41d4-a716-446655440000";
const V7: &str = "0190f0a2-7b3c-7d4e-8f00-123456789abc";

/// An email address: lower case, with one `@`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Email(String);

impl ValueObject for Email {
    type Value = String;

    fn validate(value: &String) -> Result<bool, BoxError> {
        if value.contains(' ') {
            return Err(FrameworkError::custom(
                "EMAIL_HAS_SPACES",
                ErrorCategory::Domain,
                "an email address has no spaces",
            )
            .into());
        }
        Ok(value.matches('@').count() == 1 && *value == value.to_lowercase())
    }

    fn from_validated(value: String) -> Self {
        Self(value)
    }

    fn value(&self) -> &String {
        &self.0
    }
}

/// A port number parsed from text, which fails with a foreign error.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Port(String);

impl ValueObject for Port {
    type Value = String;

    fn validate(value: &String) -> Result<bool, BoxError> {
        let port: u16 = value.parse()?;
        Ok(port != 0)
    }

    fn from_validated(value: String) -> Self {
        Self(value)
    }

    fn value(&self) -> &String {
        &self.0
    }
}

aggregate_id! {
    /// Identifies a bank account.
    pub AccountId
}

aggregate_id!(TransferId);

#[test]
fn valid_values_are_wrapped() {
    let email = Email::new("ana@example.com".to_owned()).unwrap();
    assert_eq!(email.value(), "ana@example.com");
}

#[test]
fn false_from_validate_is_value_object_invalid() {
    let error = Email::new("ANA@example.com".to_owned()).unwrap_err();
    assert_eq!(*error.code(), ErrorCode::ValueObjectInvalid);
    assert_eq!(error.http_status(), 400);
    let name = error.context()["valueObject"].as_str().unwrap();
    assert!(name.ends_with("Email"), "{name}");
}

#[test]
fn framework_errors_pass_through() {
    let error = Email::new("ana @example.com".to_owned()).unwrap_err();
    assert_eq!(error.code().as_str(), "EMAIL_HAS_SPACES");
    assert_eq!(error.category(), ErrorCategory::Domain);
}

#[test]
fn other_errors_are_construction_failures() {
    let error = Port::new("http".to_owned()).unwrap_err();
    assert_eq!(*error.code(), ErrorCode::ValueObjectConstructionFailed);
    assert!(error.source().unwrap().is::<ParseIntError>());

    let error = Port::new("0".to_owned()).unwrap_err();
    assert_eq!(*error.code(), ErrorCode::ValueObjectInvalid);
    assert_eq!(Port::new("8080".to_owned()).unwrap().value(), "8080");
}

#[test]
fn with_validates_again_and_leaves_the_original() {
    let email = Email::new("ana@example.com".to_owned()).unwrap();
    let moved = email.with(|v| v.replace("example", "acme")).unwrap();
    assert_eq!(moved.value(), "ana@acme.com");
    assert_eq!(email.value(), "ana@example.com");

    let error = email.with(|v| v.to_uppercase()).unwrap_err();
    assert_eq!(*error.code(), ErrorCode::ValueObjectInvalid);
}

#[test]
fn equality_and_hash_are_structural() {
    let a = Email::new("ana@example.com".to_owned()).unwrap();
    let b = Email::new("ana@example.com".to_owned()).unwrap();
    assert_eq!(a, b);
    assert_eq!(HashSet::from([a, b]).len(), 1);
}

#[test]
fn aggregate_id_equality_ignores_case() {
    let lower = AggregateId::from_existing(V7).unwrap();
    let upper = AggregateId::from_existing(&V7.to_uppercase()).unwrap();
    assert_eq!(lower, upper);
    assert_eq!(upper.to_string(), V7);
    assert_eq!(format!("{upper:?}"), format!("AggregateId({V7})"));
}

#[test]
fn aggregate_id_factories() {
    assert!(AggregateId::generate().uuid().get_version_num() == 7);
    assert_ne!(AggregateId::generate(), AggregateId::generate());
    assert_eq!(AggregateId::from_existing(V4).unwrap().to_string(), V4);
    assert_eq!(AggregateId::from_uuidv7(V7).unwrap().to_string(), V7);

    let error = AggregateId::from_uuidv7(V4).unwrap_err();
    assert_eq!(*error.code(), ErrorCode::ValueObjectInvalid);
    assert_eq!(error.context()["value"], V4);

    for bad in ["", "not-a-uuid", "550e8400e29b41d4a716446655440000"] {
        let error = AggregateId::from_existing(bad).unwrap_err();
        assert_eq!(*error.code(), ErrorCode::ValueObjectInvalid, "{bad:?}");
    }
    let error = "nope".parse::<AggregateId>().unwrap_err();
    assert_eq!(*error.code(), ErrorCode::ValueObjectInvalid);
}

#[test]
fn aggregate_id_serializes_as_a_string() {
    let id = AggregateId::from_existing(&V7.to_uppercase()).unwrap();
    assert_eq!(serde_json::to_value(id).unwrap(), json!(V7));
    assert_eq!(
        serde_json::from_value::<AggregateId>(json!(V7)).unwrap(),
        id
    );
    assert!(serde_json::from_value::<AggregateId>(json!("nope")).is_err());
    assert!(serde_json::from_value::<AggregateId>(json!(7)).is_err());
}

#[test]
fn typed_ids_have_the_same_factories() {
    let id = AccountId::generate();
    assert_eq!(id.uuid().get_version_num(), 7);
    assert_eq!(AccountId::from_existing(&id.to_string()).unwrap(), id);
    assert_eq!(id.to_string().parse::<AccountId>().unwrap(), id);
    assert_eq!(AccountId::from_uuid(id.uuid()), id);
    assert_eq!(AggregateId::from(id), id.as_aggregate_id());

    let error = AccountId::from_uuidv7(V4).unwrap_err();
    assert_eq!(*error.code(), ErrorCode::ValueObjectInvalid);
    assert_eq!(
        TransferId::from_existing(&V7.to_uppercase()).unwrap(),
        TransferId::from_existing(V7).unwrap()
    );
}

#[test]
fn typed_ids_format_and_serialize_like_the_uuid() {
    let id = AccountId::from_existing(V7).unwrap();
    assert_eq!(id.to_string(), V7);
    assert_eq!(format!("{id:?}"), format!("AccountId({V7})"));
    assert_eq!(serde_json::to_value(id).unwrap(), json!(V7));
    assert_eq!(serde_json::from_value::<AccountId>(json!(V7)).unwrap(), id);
    assert!(serde_json::from_value::<AccountId>(json!("nope")).is_err());
}
