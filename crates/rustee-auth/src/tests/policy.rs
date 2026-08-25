use super::*;

#[tokio::test]
async fn auth_user_receives_only_the_validated_principal() {
    let service = AuthLayer::bearer(authenticator()).layer(
        App::new().get("/me", |AuthUser(principal): AuthUser| async move {
            principal.subject().to_owned()
        }),
    );

    let response = service.oneshot(request(Some("local-token"))).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn require_auth_rejects_a_route_without_an_authenticated_principal() {
    let app = App::new().get("/me", |RequireAuth(principal): RequireAuth| async move {
        principal.subject().to_owned()
    });

    let response = app.oneshot(request(None)).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn scope_layer_rejects_an_authenticated_principal_without_every_scope() {
    let policy = RequireScopesLayer::new(["profile:read", "profile:write"]).unwrap();
    let service = AuthLayer::bearer(authenticator())
        .layer(policy.layer(App::new().get("/me", || async { "unexpected" })));

    let response = service.oneshot(request(Some("local-token"))).await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[test]
fn scope_policy_rejects_an_empty_requirement() {
    let error = RequireScopesLayer::new(Vec::<String>::new()).unwrap_err();
    assert_eq!(error, ScopePolicyError::EmptyRequirement);
}

#[test]
fn scope_policy_rejects_a_requirement_that_no_principal_can_satisfy() {
    let scopes = (0..=MAX_PRINCIPAL_AUTHORIZATION_VALUES)
        .map(|index| format!("scope-{index}"))
        .chain(std::iter::once_with(|| {
            panic!("scope policy must reject before reading the iterator tail")
        }));

    assert_eq!(
        RequireScopesLayer::new(scopes),
        Err(ScopePolicyError::TooManyScopes {
            max_values: MAX_PRINCIPAL_AUTHORIZATION_VALUES,
        })
    );
}

#[test]
fn authorization_policy_debug_redacts_configuration_values() {
    let scopes = RequireScopesLayer::new(["private.scope.read", "private.scope.write"]).unwrap();
    let mut roles = RolePolicy::new();
    roles
        .grant(
            "private-role",
            ["private.permission.read", "private.permission.write"],
        )
        .unwrap();
    let permissions =
        RequirePermissionsLayer::new(["private.permission.read"], roles.clone()).unwrap();

    let diagnostics = [
        format!("{scopes:?}"),
        format!(
            "{:?}",
            scopes.layer(App::new().get("/scope", || async { "ok" }))
        ),
        format!("{roles:?}"),
        format!("{permissions:?}"),
        format!(
            "{:?}",
            permissions.layer(App::new().get("/permission", || async { "ok" }))
        ),
    ];

    for diagnostic in diagnostics {
        assert!(!diagnostic.contains("private.scope"), "{diagnostic}");
        assert!(!diagnostic.contains("private-role"), "{diagnostic}");
        assert!(!diagnostic.contains("private.permission"), "{diagnostic}");
    }
}

#[tokio::test]
async fn permission_layer_accepts_direct_permissions_and_server_configured_roles() {
    let direct_principal = Principal::new("direct")
        .unwrap()
        .with_permission("project:read")
        .unwrap();
    let role_principal = Principal::new("role")
        .unwrap()
        .with_role("project-viewer")
        .unwrap();
    let mut authenticator = StaticTokenAuthenticator::new();
    authenticator
        .insert("direct-token", direct_principal)
        .unwrap();
    authenticator.insert("role-token", role_principal).unwrap();

    let mut roles = RolePolicy::new();
    roles.grant("project-viewer", ["project:read"]).unwrap();
    let policy = RequirePermissionsLayer::new(["project:read"], roles).unwrap();
    let service = AuthLayer::bearer(authenticator)
        .layer(policy.layer(App::new().get("/me", || async { "allowed" })));

    assert_eq!(
        service
            .clone()
            .oneshot(request(Some("direct-token")))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        service
            .oneshot(request(Some("role-token")))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn permission_layer_rejects_a_principal_without_a_grant() {
    let policy = RequirePermissionsLayer::new(["project:write"], RolePolicy::new()).unwrap();
    let service = AuthLayer::bearer(authenticator())
        .layer(policy.layer(App::new().get("/me", || async { "unexpected" })));

    let response = service.oneshot(request(Some("local-token"))).await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[test]
fn authorization_policies_reject_values_that_a_principal_cannot_hold() {
    let oversized = "x".repeat(MAX_PRINCIPAL_AUTHORIZATION_VALUE_BYTES + 1);
    let mut roles = RolePolicy::new();

    assert_eq!(
        RequireScopesLayer::new([oversized.clone()]).unwrap_err(),
        ScopePolicyError::ScopeTooLong {
            max_bytes: MAX_PRINCIPAL_AUTHORIZATION_VALUE_BYTES,
        }
    );
    assert_eq!(
        roles
            .grant(oversized.clone(), ["project:read"])
            .unwrap_err(),
        RolePolicyError::RoleTooLong {
            max_bytes: MAX_PRINCIPAL_AUTHORIZATION_VALUE_BYTES,
        }
    );
    assert_eq!(
        roles.grant("reader", [oversized.clone()]).unwrap_err(),
        RolePolicyError::PermissionTooLong {
            max_bytes: MAX_PRINCIPAL_AUTHORIZATION_VALUE_BYTES,
        }
    );
    assert_eq!(
        RequirePermissionsLayer::new([oversized], RolePolicy::new()).unwrap_err(),
        PermissionPolicyError::PermissionTooLong {
            max_bytes: MAX_PRINCIPAL_AUTHORIZATION_VALUE_BYTES,
        }
    );
}
