//! Value objects (spec §2.3): objects defined by their value, not identity.

use std::any::type_name;

use crate::error::{BoxError, FrameworkError, Result};

/// An immutable object defined by its value, such as an amount of money or an
/// email address (spec §2.3).
///
/// Implement it on a newtype and derive `Clone`, `PartialEq`, `Eq` and `Hash`
/// for structural equality. Derive `PartialOrd` and `Ord` only if the value is
/// ordered: comparing an unordered value object is then a compile error,
/// where the spec throws `INCOMPARABLE_VALUE`. The compiler also enforces the
/// immutability: a value object hands out only `&Self::Value`.
///
/// Build instances with [`new`](Self::new), which runs [`validate`](Self::validate):
///
/// - `Ok(true)` accepts the value;
/// - `Ok(false)` rejects it with `VALUE_OBJECT_INVALID`;
/// - an `Err` holding a [`FrameworkError`] rejects it with that error;
/// - any other `Err` rejects it with `VALUE_OBJECT_CONSTRUCTION_FAILED`, with
///   the error as its source.
///
/// ```
/// use tarodomain::domain::ValueObject;
/// use tarodomain::error::{BoxError, ErrorCode};
///
/// #[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
/// struct Cents(i64);
///
/// impl ValueObject for Cents {
///     type Value = i64;
///
///     fn validate(value: &i64) -> Result<bool, BoxError> {
///         Ok(*value >= 0)
///     }
///
///     fn from_validated(value: i64) -> Self {
///         Self(value)
///     }
///
///     fn value(&self) -> &i64 {
///         &self.0
///     }
/// }
///
/// let price = Cents::new(250).unwrap();
/// assert_eq!(price.with(|cents| cents * 2).unwrap(), Cents::new(500).unwrap());
/// let error = price.with(|cents| cents - 1_000).unwrap_err();
/// assert_eq!(*error.code(), ErrorCode::ValueObjectInvalid);
/// ```
pub trait ValueObject: Sized {
    /// The inner value.
    type Value;

    /// Checks the invariants of `value`. See the trait docs for what each
    /// outcome means.
    fn validate(value: &Self::Value) -> std::result::Result<bool, BoxError>;

    /// Wraps a value that [`validate`](Self::validate) accepted.
    ///
    /// Only [`new`](Self::new) should call it. Everything else goes through
    /// `new`, so that no instance skips validation.
    fn from_validated(value: Self::Value) -> Self;

    /// The inner value.
    fn value(&self) -> &Self::Value;

    /// Validates `value` and wraps it.
    fn new(value: Self::Value) -> Result<Self> {
        match Self::validate(&value) {
            Ok(true) => Ok(Self::from_validated(value)),
            Ok(false) => Err(FrameworkError::value_object_invalid(format!(
                "invalid {}",
                type_name::<Self>()
            ))
            .with_context("valueObject", type_name::<Self>())),
            Err(error) => Err(match error.downcast::<FrameworkError>() {
                Ok(error) => *error,
                Err(error) => FrameworkError::value_object_construction_failed(format!(
                    "building {} failed: {error}",
                    type_name::<Self>()
                ))
                .with_context("valueObject", type_name::<Self>())
                .with_source(error),
            }),
        }
    }

    /// A new instance from a changed copy of the value, validated again.
    /// `self` doesn't change.
    fn with(&self, f: impl FnOnce(Self::Value) -> Self::Value) -> Result<Self>
    where
        Self::Value: Clone,
    {
        Self::new(f(self.value().clone()))
    }
}
