//! Event families: the closed set of events one aggregate raises and applies,
//! as an enum (spec §5.2, Rust design decisions).

use std::fmt;

use super::{DomainEvent, EventEnvelope};

/// The events of one aggregate, as an enum with one variant per event type.
///
/// An aggregate's `apply` is an exhaustive `match` over its family, so the
/// compiler checks that every event is handled. Declare a family with
/// [`event_family!`](crate::event_family) rather than implementing this by
/// hand.
pub trait EventFamily: Sized + fmt::Debug + Send + Sync + 'static {
    /// The names of the family's events, in declaration order.
    fn names() -> &'static [&'static str];

    /// The name of this event.
    fn name(&self) -> &'static str;

    /// The event with `envelope`, type-erased. The payload isn't copied.
    fn into_domain(self, envelope: EventEnvelope) -> DomainEvent;

    /// The family member `event` holds, or `None` if its type isn't in the
    /// family. The payload isn't copied.
    fn from_domain(event: &DomainEvent) -> Option<Self>;
}

/// Declares an event family: an enum with one variant per event type, named
/// after the type and holding it in an `Arc`.
///
/// The macro also implements `From<E>` for each event type, so
/// `AccountEvent::from(MoneyDeposited { cents: 5 })` and `.into()` work, and
/// [`EventFamily`]. The event types must be in scope by their plain names.
/// The enum derives `Clone` and `Debug`; add other derives as attributes,
/// which then need the same derive on every event.
///
/// ```
/// use serde::{Deserialize, Serialize};
/// use tarodomain::domain::{Event, EventFamily};
/// use tarodomain::event_family;
///
/// #[derive(Debug, Serialize, Deserialize)]
/// struct AccountOpened {
///     owner: String,
/// }
///
/// impl Event for AccountOpened {
///     const NAME: &'static str = "AccountOpened";
/// }
///
/// #[derive(Debug, Serialize, Deserialize)]
/// struct MoneyDeposited {
///     cents: i64,
/// }
///
/// impl Event for MoneyDeposited {
///     const NAME: &'static str = "MoneyDeposited";
/// }
///
/// event_family! {
///     /// Everything that happens to a bank account.
///     pub enum AccountEvent {
///         AccountOpened,
///         MoneyDeposited,
///     }
/// }
///
/// let event = AccountEvent::from(MoneyDeposited { cents: 5 });
/// let cents = match &event {
///     AccountEvent::AccountOpened(_) => 0,
///     AccountEvent::MoneyDeposited(deposited) => deposited.cents,
/// };
/// assert_eq!(cents, 5);
/// assert_eq!(AccountEvent::names(), ["AccountOpened", "MoneyDeposited"]);
/// ```
#[macro_export]
macro_rules! event_family {
    (
        $(#[$meta:meta])*
        $vis:vis enum $name:ident {
            $($event:ident),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Clone, Debug)]
        $vis enum $name {
            $(
                #[doc = ::core::concat!("A [`", ::core::stringify!($event), "`] event.")]
                $event(::std::sync::Arc<$event>),
            )+
        }

        $(
            impl ::core::convert::From<$event> for $name {
                fn from(event: $event) -> Self {
                    Self::$event(::std::sync::Arc::new(event))
                }
            }
        )+

        impl $crate::domain::EventFamily for $name {
            fn names() -> &'static [&'static str] {
                &[$(<$event as $crate::domain::Event>::NAME),+]
            }

            fn name(&self) -> &'static str {
                match self {
                    $(Self::$event(_) => <$event as $crate::domain::Event>::NAME,)+
                }
            }

            fn into_domain(
                self,
                envelope: $crate::domain::EventEnvelope,
            ) -> $crate::domain::DomainEvent {
                match self {
                    $(
                        Self::$event(event) => $crate::domain::DomainEvent::from(
                            $crate::domain::Recorded { envelope, event },
                        ),
                    )+
                }
            }

            fn from_domain(
                event: &$crate::domain::DomainEvent,
            ) -> ::core::option::Option<Self> {
                $(
                    if event.is::<$event>() {
                        return event
                            .downcast::<$event>()
                            .map(|recorded| Self::$event(recorded.event));
                    }
                )+
                ::core::option::Option::None
            }
        }
    };
}
