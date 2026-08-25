use std::{net::SocketAddr, sync::Arc};

use crate::*;

struct PrivateState(&'static str);

#[test]
fn state_store_is_type_indexed() {
    let mut store = StateStore::default();
    store.insert(String::from("configured"));
    assert_eq!(
        store.get::<String>().as_deref(),
        Some(&String::from("configured"))
    );
    assert!(store.get::<u64>().is_none());
}

#[test]
fn state_debug_output_is_type_only_and_does_not_require_state_debug() {
    let state = State(Arc::new(PrivateState("private-application-state")));

    let output = format!("{state:?}");

    assert!(output.contains("State"));
    assert!(output.contains(std::any::type_name::<PrivateState>()));
    assert!(!output.contains(state.0.0));
}

#[test]
fn route_params_debug_output_redacts_user_controlled_values() {
    let params = RouteParams::new(vec![
        ("account".to_owned(), "private-account-id".to_owned()),
        ("document".to_owned(), "private-document-id".to_owned()),
    ]);

    let output = format!("{params:?}");

    assert!(output.contains("parameter_count: 2"));
    assert!(!output.contains("account"));
    assert!(!output.contains("private-account-id"));
    assert!(!output.contains("private-document-id"));
}

#[test]
fn connection_info_debug_output_redacts_the_peer_address() {
    let connection = ConnectionInfo::new(SocketAddr::from(([203, 0, 113, 42], 443)));

    let output = format!("{connection:?}");

    assert!(output.contains("peer_addr: \"[REDACTED]\""));
    assert!(!output.contains("203.0.113.42"));
}

#[test]
fn request_extractor_debug_output_redacts_user_controlled_values() {
    let outputs = [
        format!("{:?}", Json("private JSON body")),
        format!("{:?}", Query("private query value")),
        format!("{:?}", Path("private path value")),
        format!("{:?}", Header("private header value")),
    ];

    for output in outputs {
        assert!(output.contains("[REDACTED]"));
        assert!(!output.contains("private JSON body"));
        assert!(!output.contains("private query value"));
        assert!(!output.contains("private path value"));
        assert!(!output.contains("private header value"));
    }
}
