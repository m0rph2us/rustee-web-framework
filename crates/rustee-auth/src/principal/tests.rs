//! Regression coverage for principal validation, normalization, and redaction.

use super::{
    MAX_PRINCIPAL_AUTHORIZATION_VALUE_BYTES, MAX_PRINCIPAL_AUTHORIZATION_VALUES,
    MAX_PRINCIPAL_IDENTIFIER_BYTES, Principal, PrincipalError,
};

#[test]
fn debug_redacts_identity_and_authorization_values() {
    let principal = Principal::new("alice@example.test")
        .unwrap()
        .with_issuer("https://issuer.example.test")
        .unwrap()
        .with_tenant("tenant-a")
        .unwrap()
        .with_scope("profile:read")
        .unwrap()
        .with_role("operator")
        .unwrap()
        .with_permission("reports:read")
        .unwrap();

    let output = format!("{principal:?}");

    for value in [
        "alice@example.test",
        "https://issuer.example.test",
        "tenant-a",
        "profile:read",
        "operator",
        "reports:read",
    ] {
        assert!(!output.contains(value));
    }
    assert!(output.contains("scope_count: 1"));
    assert!(output.contains("role_count: 1"));
    assert!(output.contains("permission_count: 1"));
}

#[test]
fn principal_rejects_oversized_identity_and_authorization_values() {
    assert_eq!(
        Principal::new("x".repeat(MAX_PRINCIPAL_IDENTIFIER_BYTES + 1)),
        Err(PrincipalError::ValueTooLong {
            field: "subject",
            max_bytes: MAX_PRINCIPAL_IDENTIFIER_BYTES,
        })
    );
    assert_eq!(
        Principal::new("alice")
            .unwrap()
            .with_scope("x".repeat(MAX_PRINCIPAL_AUTHORIZATION_VALUE_BYTES + 1)),
        Err(PrincipalError::ValueTooLong {
            field: "scopes",
            max_bytes: MAX_PRINCIPAL_AUTHORIZATION_VALUE_BYTES,
        })
    );
}

#[test]
fn principal_rejects_a_new_authorization_value_after_its_set_is_full() {
    let principal = (0..MAX_PRINCIPAL_AUTHORIZATION_VALUES)
        .try_fold(Principal::new("alice").unwrap(), |principal, value| {
            principal.with_scope(format!("scope-{value}"))
        });
    let principal = principal.unwrap().with_scope("scope-0").unwrap();

    assert_eq!(
        principal.with_scope("scope-overflow"),
        Err(PrincipalError::TooManyValues {
            field: "scopes",
            max_values: MAX_PRINCIPAL_AUTHORIZATION_VALUES,
        })
    );
}

#[test]
fn deserialization_revalidates_principal_content() {
    let serialized = serde_json::json!({
        "subject": "alice",
        "issuer": null,
        "tenant": null,
        "scopes": (0..=MAX_PRINCIPAL_AUTHORIZATION_VALUES)
            .map(|value| format!("scope-{value}"))
            .collect::<Vec<_>>(),
        "roles": [],
        "permissions": [],
    });

    let error = serde_json::from_value::<Principal>(serialized).unwrap_err();

    let expected = format!("scopes exceeds the {MAX_PRINCIPAL_AUTHORIZATION_VALUES}-value limit");
    assert!(error.to_string().contains(&expected));
}

#[test]
fn verified_claim_normalization_applies_the_shared_principal_contract() {
    let principal = Principal::from_verified_claims(
        "alice".to_owned(),
        "https://issuer.example.test".to_owned(),
        Some("tenant-a".to_owned()),
        ["profile:read".to_owned()],
        ["operator".to_owned()],
        ["reports:read".to_owned()],
    )
    .unwrap();

    assert_eq!(principal.subject(), "alice");
    assert_eq!(principal.issuer(), Some("https://issuer.example.test"));
    assert_eq!(principal.tenant(), Some("tenant-a"));
    assert!(principal.has_scope("profile:read"));
    assert!(principal.has_role("operator"));
    assert!(principal.has_permission("reports:read"));
}
