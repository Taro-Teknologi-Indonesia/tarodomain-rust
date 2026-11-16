//! Event families (`event_family!`), used from outside the crate so that the
//! macro's paths are checked too.

use std::sync::Arc;

use chrono::DateTime;
use serde::{Deserialize, Serialize};
use tarodomain::domain::{DomainEvent, Event, EventEnvelope, EventFamily};
use tarodomain::event_family;

/// A bank account was opened.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct AccountOpened {
    owner: String,
}

impl Event for AccountOpened {
    const NAME: &'static str = "AccountOpened";
}

/// Money was paid in.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct MoneyDeposited {
    cents: i64,
}

impl Event for MoneyDeposited {
    const NAME: &'static str = "MoneyDeposited";
}

/// Money was paid out.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct MoneyWithdrawn {
    cents: i64,
}

impl Event for MoneyWithdrawn {
    const NAME: &'static str = "BankAccount.MoneyWithdrawn";
    const VERSION: &'static str = "2.0.0";
}

/// An event of another aggregate.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct TransferRequested {
    cents: i64,
}

impl Event for TransferRequested {
    const NAME: &'static str = "TransferRequested";
}

event_family! {
    /// Everything that happens to a bank account.
    #[derive(PartialEq)]
    pub enum AccountEvent {
        AccountOpened,
        MoneyDeposited,
        MoneyWithdrawn,
    }
}

event_family!(
    enum TransferEvent {
        TransferRequested,
    }
);

fn envelope() -> EventEnvelope {
    let at = DateTime::from_timestamp_millis(1_763_280_000_123).unwrap();
    EventEnvelope::from_context(None, at)
}

#[test]
fn names_follow_declaration_order() {
    assert_eq!(
        AccountEvent::names(),
        [
            "AccountOpened",
            "MoneyDeposited",
            "BankAccount.MoneyWithdrawn"
        ]
    );
    assert_eq!(TransferEvent::names(), ["TransferRequested"]);
}

#[test]
fn each_event_converts_into_its_variant() {
    let opened = AccountEvent::from(AccountOpened {
        owner: "ana".to_owned(),
    });
    assert_eq!(opened.name(), "AccountOpened");
    let withdrawn: AccountEvent = MoneyWithdrawn { cents: 3 }.into();
    assert_eq!(withdrawn.name(), "BankAccount.MoneyWithdrawn");
    assert_eq!(
        withdrawn,
        AccountEvent::MoneyWithdrawn(Arc::new(MoneyWithdrawn { cents: 3 }))
    );
}

#[test]
fn into_domain_keeps_the_envelope_and_type() {
    let envelope = envelope();
    let event = AccountEvent::from(MoneyWithdrawn { cents: 3 }).into_domain(envelope.clone());
    assert_eq!(event.name(), "BankAccount.MoneyWithdrawn");
    assert_eq!(event.version(), "2.0.0");
    assert_eq!(event.envelope(), &envelope);
    assert_eq!(
        event.downcast_ref::<MoneyWithdrawn>(),
        Some(&MoneyWithdrawn { cents: 3 })
    );
}

#[test]
fn from_domain_shares_the_payload() {
    let event = DomainEvent::new(envelope(), MoneyDeposited { cents: 5 });
    let Some(AccountEvent::MoneyDeposited(deposited)) = AccountEvent::from_domain(&event) else {
        panic!("expected MoneyDeposited");
    };
    assert_eq!(deposited.cents, 5);
    let shared = event.downcast::<MoneyDeposited>().unwrap().event;
    assert!(Arc::ptr_eq(&deposited, &shared));
}

#[test]
fn round_trip() {
    let family = AccountEvent::from(AccountOpened {
        owner: "ana".to_owned(),
    });
    let back = AccountEvent::from_domain(&family.clone().into_domain(envelope()));
    assert_eq!(back, Some(family));
}

#[test]
fn an_event_outside_the_family_is_none() {
    let event = DomainEvent::new(envelope(), TransferRequested { cents: 5 });
    assert_eq!(AccountEvent::from_domain(&event), None);
    assert!(TransferEvent::from_domain(&event).is_some());

    let deposited = DomainEvent::new(envelope(), MoneyDeposited { cents: 5 });
    assert!(TransferEvent::from_domain(&deposited).is_none());
}
