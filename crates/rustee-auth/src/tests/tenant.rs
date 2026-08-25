use super::*;
use crate::MAX_TENANT_IDENTIFIER_BYTES;

#[derive(Clone, Copy)]
struct UnavailableTenantResolver;

impl TenantResolver for UnavailableTenantResolver {
    type Error = std::io::Error;

    fn resolve(
        &self,
        _: &rustee_core::Request,
        _: &Principal,
    ) -> futures_util::future::BoxFuture<'static, Result<Option<TenantContext>, Self::Error>> {
        Box::pin(future::ready(Err(std::io::Error::other("not reachable"))))
    }
}

fn tenant_authenticator() -> StaticTokenAuthenticator {
    let mut authenticator = StaticTokenAuthenticator::new();
    authenticator
        .insert(
            "tenant-token",
            Principal::new("alice")
                .unwrap()
                .with_tenant("acme")
                .unwrap(),
        )
        .unwrap();
    authenticator
}

fn tenant_request(token: Option<&str>, host: &str) -> rustee_core::Request {
    let mut request = request(token);
    request.headers_mut().insert("host", host.parse().unwrap());
    request
}

#[tokio::test]
async fn tenant_layer_allows_only_the_server_context_matching_the_principal() {
    let tenant_principal = Principal::new("alice")
        .unwrap()
        .with_tenant("acme")
        .unwrap();
    let mut authenticator = StaticTokenAuthenticator::new();
    authenticator
        .insert("tenant-token", tenant_principal)
        .unwrap();
    let service = AuthLayer::bearer(authenticator)
        .layer(RequireTenantMatchLayer::new().layer(App::new().get("/me", || async { "allowed" })));

    let mut matching = request(Some("tenant-token"));
    matching
        .extensions_mut()
        .insert(TenantContext::new("acme").unwrap());
    assert_eq!(
        service.clone().oneshot(matching).await.unwrap().status(),
        StatusCode::OK
    );

    let mut mismatched = request(Some("tenant-token"));
    mismatched
        .extensions_mut()
        .insert(TenantContext::new("other").unwrap());
    assert_eq!(
        service.clone().oneshot(mismatched).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );

    assert_eq!(
        service
            .oneshot(request(Some("tenant-token")))
            .await
            .unwrap()
            .status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
}

#[tokio::test]
async fn host_tenant_resolution_isolates_the_verified_principal() {
    let resolver = HostTenantResolver::new([
        ("acme.example.test", TenantContext::new("acme").unwrap()),
        ("other.example.test", TenantContext::new("other").unwrap()),
    ])
    .unwrap();
    let service = AuthLayer::bearer(tenant_authenticator()).layer(
        TenantResolutionLayer::new(resolver).layer(
            App::new().get("/me", |context: TenantContext| async move {
                context.tenant().to_owned()
            }),
        ),
    );

    let response = service
        .clone()
        .oneshot(tenant_request(Some("tenant-token"), "ACME.EXAMPLE.TEST"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    assert_eq!(
        service
            .clone()
            .oneshot(tenant_request(Some("tenant-token"), "other.example.test"))
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        service
            .clone()
            .oneshot(tenant_request(Some("tenant-token"), "unknown.example.test"))
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );

    let mut duplicate = tenant_request(Some("tenant-token"), "acme.example.test");
    duplicate
        .headers_mut()
        .append("host", "other.example.test".parse().unwrap());
    assert_eq!(
        service.oneshot(duplicate).await.unwrap().status(),
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn tenant_resolution_failure_is_sanitized_and_fail_closed() {
    let service = AuthLayer::bearer(tenant_authenticator()).layer(
        TenantResolutionLayer::new(UnavailableTenantResolver)
            .layer(App::new().get("/me", || async { "unexpected" })),
    );
    let response = service
        .oneshot(tenant_request(Some("tenant-token"), "acme.example.test"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[test]
fn role_and_tenant_policy_reject_invalid_configuration() {
    let mut roles = RolePolicy::new();
    assert_eq!(
        roles.grant("", ["project:read"]).unwrap_err(),
        RolePolicyError::BlankRole
    );
    assert_eq!(
        roles.grant("viewer", Vec::<String>::new()).unwrap_err(),
        RolePolicyError::EmptyPermissions
    );
    assert_eq!(
        RequirePermissionsLayer::new(Vec::<String>::new(), roles).unwrap_err(),
        PermissionPolicyError::EmptyRequirement
    );
    assert_eq!(
        TenantContext::new(" ").unwrap_err(),
        TenantPolicyError::BlankTenant
    );
    assert_eq!(
        TenantContext::new("t".repeat(MAX_TENANT_IDENTIFIER_BYTES + 1)).unwrap_err(),
        TenantPolicyError::ValueTooLong
    );
    assert_eq!(
        TenantContext::new("tenant\0invalid").unwrap_err(),
        TenantPolicyError::TenantContainsNul
    );
    assert_eq!(
        HostTenantResolver::new(Vec::<(String, TenantContext)>::new()).unwrap_err(),
        HostTenantResolverError::EmptyMapping
    );
    assert_eq!(
        HostTenantResolver::new([
            ("ACME.EXAMPLE.TEST", TenantContext::new("acme").unwrap()),
            ("acme.example.test", TenantContext::new("other").unwrap()),
        ])
        .unwrap_err(),
        HostTenantResolverError::DuplicateHost
    );
    assert_eq!(
        HostTenantResolver::new([(
            "https://acme.example.test",
            TenantContext::new("acme").unwrap(),
        )])
        .unwrap_err(),
        HostTenantResolverError::InvalidHost
    );
}
