//! Metadata (spec §2.1): free-form primitives, such as a user or tenant ID,
//! that every event and command carries.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Number, Value};

/// A metadata value: null, a bool, a number or a string.
///
/// Spec §2.1 types metadata as `Map<string, primitive>`, so arrays and objects
/// are rejected when metadata is read from JSON.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum MetadataValue {
    /// JSON `null`.
    Null,
    /// A boolean.
    Bool(bool),
    /// A number. A float that isn't finite becomes [`MetadataValue::Null`]
    /// instead, as in `serde_json`.
    Number(Number),
    /// A string.
    String(String),
}

impl MetadataValue {
    /// Whether the value is null.
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// The value as a bool, if it is one.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// The value as a string, if it is one.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(s) => Some(s),
            _ => None,
        }
    }

    /// The value as an `i64`, if it is an integer that fits.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Number(n) => n.as_i64(),
            _ => None,
        }
    }

    /// The value as a `u64`, if it is an integer that fits.
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Self::Number(n) => n.as_u64(),
            _ => None,
        }
    }

    /// The value as an `f64`, if it is a number.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Number(n) => n.as_f64(),
            _ => None,
        }
    }
}

impl From<bool> for MetadataValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<&str> for MetadataValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<String> for MetadataValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<Number> for MetadataValue {
    fn from(value: Number) -> Self {
        Self::Number(value)
    }
}

macro_rules! from_integer {
    ($($int:ty),+) => {
        $(
            impl From<$int> for MetadataValue {
                fn from(value: $int) -> Self {
                    Self::Number(value.into())
                }
            }
        )+
    };
}

from_integer!(i32, i64, u32, u64);

impl From<f64> for MetadataValue {
    /// A finite float becomes a number, and NaN or an infinity becomes null.
    fn from(value: f64) -> Self {
        Number::from_f64(value).map_or(Self::Null, Self::Number)
    }
}

impl<T: Into<MetadataValue>> From<Option<T>> for MetadataValue {
    /// `None` becomes null.
    fn from(value: Option<T>) -> Self {
        value.map_or(Self::Null, Into::into)
    }
}

impl TryFrom<Value> for MetadataValue {
    /// An array or object, handed back unchanged.
    type Error = Value;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Null => Ok(Self::Null),
            Value::Bool(b) => Ok(Self::Bool(b)),
            Value::Number(n) => Ok(Self::Number(n)),
            Value::String(s) => Ok(Self::String(s)),
            other @ (Value::Array(_) | Value::Object(_)) => Err(other),
        }
    }
}

impl From<MetadataValue> for Value {
    fn from(value: MetadataValue) -> Self {
        match value {
            MetadataValue::Null => Value::Null,
            MetadataValue::Bool(b) => Value::Bool(b),
            MetadataValue::Number(n) => Value::Number(n),
            MetadataValue::String(s) => Value::String(s),
        }
    }
}

impl Serialize for MetadataValue {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Null => serializer.serialize_unit(),
            Self::Bool(b) => serializer.serialize_bool(*b),
            Self::Number(n) => n.serialize(serializer),
            Self::String(s) => serializer.serialize_str(s),
        }
    }
}

impl<'de> Deserialize<'de> for MetadataValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::try_from(Value::deserialize(deserializer)?).map_err(|_| {
            D::Error::custom("a metadata value must be null, a bool, a number or a string")
        })
    }
}

/// Free-form metadata, such as a user ID, tenant ID or client IP (spec §2.1).
///
/// An immutable map from string keys to [`MetadataValue`]s, kept in key order.
/// Cloning is cheap: clones share one map. [`with`](Self::with) returns a new
/// map and leaves the original as it was. Serialized as a JSON object.
///
/// ```
/// use tarodomain::domain::Metadata;
///
/// let metadata = Metadata::from([("userId", "u-1")]).with("attempt", 2);
/// assert_eq!(metadata.get("userId").and_then(|v| v.as_str()), Some("u-1"));
/// assert_eq!(
///     serde_json::to_string(&metadata).unwrap(),
///     r#"{"attempt":2,"userId":"u-1"}"#
/// );
/// ```
#[derive(Clone, Default, PartialEq, Eq, Hash)]
pub struct Metadata(Arc<BTreeMap<String, MetadataValue>>);

impl Metadata {
    /// Empty metadata.
    pub fn new() -> Self {
        Self::default()
    }

    /// The value for `key`, if there is one.
    pub fn get(&self, key: &str) -> Option<&MetadataValue> {
        self.0.get(key)
    }

    /// Whether there is a value for `key`.
    pub fn contains_key(&self, key: &str) -> bool {
        self.0.contains_key(key)
    }

    /// The number of entries.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether there are no entries.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The entries, in key order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &MetadataValue)> {
        self.0.iter().map(|(key, value)| (key.as_str(), value))
    }

    /// A copy with `key` set to `value`, replacing an earlier value for `key`.
    ///
    /// Clones of `self` don't change. The map is copied only if a clone
    /// shares it.
    #[must_use]
    pub fn with(mut self, key: impl Into<String>, value: impl Into<MetadataValue>) -> Self {
        Arc::make_mut(&mut self.0).insert(key.into(), value.into());
        self
    }
}

impl fmt::Debug for Metadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

impl<K: Into<String>, V: Into<MetadataValue>> FromIterator<(K, V)> for Metadata {
    fn from_iter<I: IntoIterator<Item = (K, V)>>(entries: I) -> Self {
        let map = entries
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect();
        Self(Arc::new(map))
    }
}

impl<K: Into<String>, V: Into<MetadataValue>, const N: usize> From<[(K, V); N]> for Metadata {
    fn from(entries: [(K, V); N]) -> Self {
        entries.into_iter().collect()
    }
}

impl From<&Metadata> for Value {
    /// The metadata as a JSON object.
    fn from(metadata: &Metadata) -> Self {
        let map = metadata
            .0
            .iter()
            .map(|(key, value)| (key.clone(), value.clone().into()))
            .collect();
        Value::Object(map)
    }
}

impl Serialize for Metadata {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Metadata {
    /// Reads a JSON object whose values are all primitives.
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        BTreeMap::<String, Value>::deserialize(deserializer)?
            .into_iter()
            .map(|(key, value)| match MetadataValue::try_from(value) {
                Ok(value) => Ok((key, value)),
                Err(_) => Err(D::Error::custom(format!(
                    "metadata value `{key}` must be null, a bool, a number or a string"
                ))),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn primitives_convert() {
        assert!(MetadataValue::from(None::<&str>).is_null());
        assert_eq!(MetadataValue::from(true).as_bool(), Some(true));
        assert_eq!(MetadataValue::from("u-1").as_str(), Some("u-1"));
        assert_eq!(MetadataValue::from(-3_i32).as_i64(), Some(-3));
        assert_eq!(MetadataValue::from(7_u64).as_u64(), Some(7));
        assert_eq!(MetadataValue::from(1.5).as_f64(), Some(1.5));
        assert_eq!(MetadataValue::from(Some(2_u32)).as_u64(), Some(2));
        assert_eq!(MetadataValue::from("x").as_i64(), None);
    }

    #[test]
    fn floats_that_are_not_finite_become_null() {
        assert!(MetadataValue::from(f64::NAN).is_null());
        assert!(MetadataValue::from(f64::INFINITY).is_null());
    }

    #[test]
    fn arrays_and_objects_are_not_values() {
        assert_eq!(MetadataValue::try_from(json!([1])), Err(json!([1])));
        assert_eq!(MetadataValue::try_from(json!({})), Err(json!({})));
        assert_eq!(
            MetadataValue::try_from(json!("a")),
            Ok(MetadataValue::from("a"))
        );
    }

    #[test]
    fn with_leaves_clones_unchanged() {
        let original = Metadata::from([("tenant", "acme")]);
        let copy = original.clone();
        let changed = copy.clone().with("tenant", "globex").with("userId", 7);

        assert_eq!(original, copy);
        assert_eq!(original.len(), 1);
        assert_eq!(original.get("tenant"), Some(&"acme".into()));
        assert_eq!(changed.get("tenant"), Some(&"globex".into()));
        assert_eq!(changed.get("userId"), Some(&7.into()));
        assert!(!original.contains_key("userId"));
    }

    #[test]
    fn serializes_as_an_object_in_key_order() {
        let metadata = Metadata::from_iter([
            ("z", MetadataValue::Null),
            ("a", true.into()),
            ("m", 1.5.into()),
        ]);
        assert_eq!(
            serde_json::to_string(&metadata).unwrap(),
            r#"{"a":true,"m":1.5,"z":null}"#
        );
        assert_eq!(
            Value::from(&metadata),
            json!({"a": true, "m": 1.5, "z": null})
        );
        let metadata = Metadata::from([("b", MetadataValue::Null), ("a", true.into())]);
        assert_eq!(format!("{metadata:?}"), r#"{"a": Bool(true), "b": Null}"#);
    }

    #[test]
    fn deserializes_primitives_only() {
        let json = json!({"userId": "u-1", "admin": false, "n": 3, "none": null});
        let metadata: Metadata = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(metadata.len(), 4);
        assert_eq!(Value::from(&metadata), json);

        for bad in [json!({"tags": ["a"]}), json!({"user": {"id": 1}})] {
            let error = serde_json::from_value::<Metadata>(bad).unwrap_err();
            assert!(
                error.to_string().contains("must be null, a bool"),
                "{error}"
            );
        }
        assert!(serde_json::from_value::<Metadata>(json!([])).is_err());
    }

    #[test]
    fn empty_by_default() {
        let metadata = Metadata::new();
        assert!(metadata.is_empty());
        assert_eq!(metadata, Metadata::default());
        assert_eq!(serde_json::to_string(&metadata).unwrap(), "{}");
    }
}
