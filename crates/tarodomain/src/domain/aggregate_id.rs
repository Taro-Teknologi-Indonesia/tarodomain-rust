//! Aggregate IDs (spec §2.4): UUIDs, typed per aggregate with
//! [`aggregate_id!`](crate::aggregate_id).

use std::fmt;
use std::str::FromStr;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::{FrameworkError, Result};
use crate::kernel::ids::{Uuid, is_v7, new_v7, parse_uuid};

/// An aggregate identifier: a UUID (spec §2.4).
///
/// Displayed and serialized as a hyphenated lower-case string, so equality
/// ignores the case it was parsed from. Use [`aggregate_id!`](crate::aggregate_id)
/// to declare a typed ID per aggregate, such as `AccountId`, so that IDs of
/// different aggregates don't mix.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AggregateId(Uuid);

impl AggregateId {
    /// A new ID: a fresh UUIDv7.
    pub fn generate() -> Self {
        Self(new_v7())
    }

    /// Parses an existing ID: any UUID version, hyphenated, in either case.
    ///
    /// Fails with `VALUE_OBJECT_INVALID`.
    pub fn from_existing(text: &str) -> Result<Self> {
        parse_uuid(text).map(Self).ok_or_else(|| {
            FrameworkError::value_object_invalid(format!(
                "aggregate ID is not a valid UUID: {text:?}"
            ))
            .with_context("value", text)
        })
    }

    /// Parses an existing ID that must be a UUIDv7.
    ///
    /// Fails with `VALUE_OBJECT_INVALID`, also for a valid UUID of another
    /// version.
    pub fn from_uuidv7(text: &str) -> Result<Self> {
        let id = Self::from_existing(text)?;
        if !is_v7(&id.0) {
            return Err(FrameworkError::value_object_invalid(format!(
                "aggregate ID is not a UUIDv7: {text:?}"
            ))
            .with_context("value", text));
        }
        Ok(id)
    }

    /// Wraps a UUID.
    pub const fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// The UUID.
    pub const fn uuid(&self) -> Uuid {
        self.0
    }
}

impl fmt::Display for AggregateId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl fmt::Debug for AggregateId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AggregateId({})", self.0)
    }
}

impl FromStr for AggregateId {
    type Err = FrameworkError;

    /// Same as [`AggregateId::from_existing`].
    fn from_str(text: &str) -> Result<Self> {
        Self::from_existing(text)
    }
}

impl From<Uuid> for AggregateId {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl From<AggregateId> for Uuid {
    fn from(id: AggregateId) -> Self {
        id.0
    }
}

impl Serialize for AggregateId {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.collect_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for AggregateId {
    /// Reads the string form, as [`AggregateId::from_existing`] parses it.
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        Self::from_existing(&text).map_err(|error| D::Error::custom(error.message()))
    }
}

/// Declares a typed aggregate ID: a newtype over [`AggregateId`] with the
/// same factories (spec §2.4: `class AccountId extends AggregateId`).
///
/// ```
/// use tarodomain::aggregate_id;
/// use tarodomain::domain::AggregateId;
///
/// aggregate_id! {
///     /// Identifies a bank account.
///     pub AccountId
/// }
///
/// let id = AccountId::generate();
/// let parsed: AccountId = id.to_string().parse().unwrap();
/// assert_eq!(parsed, id);
/// let untyped: AggregateId = id.into();
/// assert_eq!(untyped.uuid(), id.uuid());
/// ```
///
/// IDs of different aggregates don't mix:
///
/// ```compile_fail
/// use tarodomain::aggregate_id;
///
/// aggregate_id!(pub AccountId);
/// aggregate_id!(pub TransferId);
///
/// fn close(account: AccountId) {}
/// close(TransferId::generate());
/// ```
///
/// The generated type is `Copy`, `Eq`, `Ord` and `Hash`, displays and
/// serializes as the lower-case UUID, parses with `str::parse`, and converts
/// into `AggregateId`. It needs no dependencies in the calling crate besides
/// `tarodomain`.
#[macro_export]
macro_rules! aggregate_id {
    ($(#[$meta:meta])* $vis:vis $name:ident $(;)?) => {
        $(#[$meta])*
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        $vis struct $name($crate::domain::AggregateId);

        impl $name {
            /// A new ID: a fresh UUIDv7.
            pub fn generate() -> Self {
                Self($crate::domain::AggregateId::generate())
            }

            /// Parses an existing ID: any UUID version, hyphenated, in
            /// either case. Fails with `VALUE_OBJECT_INVALID`.
            pub fn from_existing(text: &str) -> $crate::error::Result<Self> {
                $crate::domain::AggregateId::from_existing(text).map(Self)
            }

            /// Parses an existing ID that must be a UUIDv7. Fails with
            /// `VALUE_OBJECT_INVALID`.
            pub fn from_uuidv7(text: &str) -> $crate::error::Result<Self> {
                $crate::domain::AggregateId::from_uuidv7(text).map(Self)
            }

            /// Wraps a UUID.
            pub const fn from_uuid(uuid: $crate::kernel::ids::Uuid) -> Self {
                Self($crate::domain::AggregateId::from_uuid(uuid))
            }

            /// The UUID.
            pub const fn uuid(&self) -> $crate::kernel::ids::Uuid {
                self.0.uuid()
            }

            /// The untyped ID.
            pub const fn as_aggregate_id(&self) -> $crate::domain::AggregateId {
                self.0
            }
        }

        impl ::core::fmt::Display for $name {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                ::core::fmt::Display::fmt(&self.0, f)
            }
        }

        impl ::core::fmt::Debug for $name {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                ::core::write!(f, "{}({})", ::core::stringify!($name), self.0)
            }
        }

        impl ::core::str::FromStr for $name {
            type Err = $crate::error::FrameworkError;

            fn from_str(text: &str) -> $crate::error::Result<Self> {
                Self::from_existing(text)
            }
        }

        impl ::core::convert::From<$name> for $crate::domain::AggregateId {
            fn from(id: $name) -> Self {
                id.0
            }
        }

        impl $crate::__private::serde::Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> ::core::result::Result<S::Ok, S::Error>
            where
                S: $crate::__private::serde::Serializer,
            {
                $crate::__private::serde::Serialize::serialize(&self.0, serializer)
            }
        }

        impl<'de> $crate::__private::serde::Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> ::core::result::Result<Self, D::Error>
            where
                D: $crate::__private::serde::Deserializer<'de>,
            {
                <$crate::domain::AggregateId as $crate::__private::serde::Deserialize<'de>>::deserialize(
                    deserializer,
                )
                .map(Self)
            }
        }
    };
}
