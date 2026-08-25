use http::StatusCode;
use serde_json::json;

use crate::{
    OpenApiApiKeyLocation, OpenApiDocument, OpenApiError, OpenApiMethod, OpenApiOAuthFlow,
    OpenApiOperation, OpenApiRoute, OpenApiSecurityRequirement, OpenApiSecurityScheme,
};

#[test]
fn document_renders_explicit_security_schemes_and_requirements() {
    let document = OpenApiDocument::new("Todo API", "0.1.0")
        .unwrap()
        .security_scheme(
            "bearerAuth",
            OpenApiSecurityScheme::http_bearer_with_format("JWT").unwrap(),
        )
        .unwrap()
        .security_scheme(
            "oidc",
            OpenApiSecurityScheme::open_id_connect(
                "https://issuer.example/.well-known/openid-configuration",
            )
            .unwrap(),
        )
        .unwrap()
        .security_scheme(
            "tenantKey",
            OpenApiSecurityScheme::api_key("X-Tenant-Key", OpenApiApiKeyLocation::Header).unwrap(),
        )
        .unwrap()
        .security_scheme("serviceTls", OpenApiSecurityScheme::mutual_tls())
        .unwrap()
        .operation(
            OpenApiRoute::from_rustee("/todos").unwrap(),
            OpenApiMethod::Get,
            OpenApiOperation::builder("list_todos")
                .security_requirement(OpenApiSecurityRequirement::scheme("bearerAuth").unwrap())
                .security_requirement(
                    OpenApiSecurityRequirement::scoped("oidc", ["todos.read"])
                        .unwrap()
                        .and_scheme("tenantKey")
                        .unwrap(),
                )
                .empty_response(StatusCode::OK, "Todos")
                .build()
                .unwrap(),
        )
        .unwrap()
        .to_value();

    assert_eq!(
        document["components"]["securitySchemes"]["bearerAuth"],
        json!({ "type": "http", "scheme": "bearer", "bearerFormat": "JWT" })
    );
    assert_eq!(
        document["components"]["securitySchemes"]["tenantKey"],
        json!({ "type": "apiKey", "name": "X-Tenant-Key", "in": "header" })
    );
    assert_eq!(
        document["components"]["securitySchemes"]["serviceTls"],
        json!({ "type": "mutualTLS" })
    );
    assert_eq!(
        document["paths"]["/todos"]["get"]["security"],
        json!([
            { "bearerAuth": [] },
            { "oidc": ["todos.read"], "tenantKey": [] },
        ])
    );
}

#[test]
fn security_requirements_fail_closed_on_unknown_or_incompatible_schemes() {
    let unknown = OpenApiOperation::builder("list_todos")
        .security_requirement(OpenApiSecurityRequirement::scheme("unknown").unwrap())
        .empty_response(StatusCode::OK, "Todos")
        .build()
        .unwrap();
    assert_eq!(
        OpenApiDocument::new("Todo API", "0.1.0")
            .unwrap()
            .operation(
                OpenApiRoute::from_rustee("/todos").unwrap(),
                OpenApiMethod::Get,
                unknown,
            )
            .unwrap_err(),
        OpenApiError::UnknownSecurityScheme
    );

    let scoped_bearer = OpenApiOperation::builder("list_todos")
        .security_requirement(
            OpenApiSecurityRequirement::scoped("bearerAuth", ["todos.read"]).unwrap(),
        )
        .empty_response(StatusCode::OK, "Todos")
        .build()
        .unwrap();
    assert_eq!(
        OpenApiDocument::new("Todo API", "0.1.0")
            .unwrap()
            .security_scheme("bearerAuth", OpenApiSecurityScheme::http_bearer())
            .unwrap()
            .operation(
                OpenApiRoute::from_rustee("/todos").unwrap(),
                OpenApiMethod::Get,
                scoped_bearer,
            )
            .unwrap_err(),
        OpenApiError::SecurityScopesNotAllowed
    );

    let scoped_mutual_tls = OpenApiOperation::builder("list_todos")
        .security_requirement(
            OpenApiSecurityRequirement::scoped("serviceTls", ["todos.read"]).unwrap(),
        )
        .empty_response(StatusCode::OK, "Todos")
        .build()
        .unwrap();
    assert_eq!(
        OpenApiDocument::new("Todo API", "0.1.0")
            .unwrap()
            .security_scheme("serviceTls", OpenApiSecurityScheme::mutual_tls())
            .unwrap()
            .operation(
                OpenApiRoute::from_rustee("/todos").unwrap(),
                OpenApiMethod::Get,
                scoped_mutual_tls,
            )
            .unwrap_err(),
        OpenApiError::SecurityScopesNotAllowed
    );
}

#[test]
fn security_scheme_validation_rejects_duplicate_or_unsafe_metadata() {
    assert!(OpenApiSecurityScheme::api_key("X-Tenant-Key", OpenApiApiKeyLocation::Header).is_ok());
    assert_eq!(
        OpenApiSecurityScheme::api_key("x tenant key", OpenApiApiKeyLocation::Header).unwrap_err(),
        OpenApiError::InvalidMetadata {
            field: "API key header name",
        }
    );
    let query_document = OpenApiDocument::new("Todo API", "0.1.0")
        .unwrap()
        .security_scheme(
            "queryKey",
            OpenApiSecurityScheme::api_key("tenant key", OpenApiApiKeyLocation::Query).unwrap(),
        )
        .unwrap()
        .to_value();
    assert_eq!(
        query_document["components"]["securitySchemes"]["queryKey"],
        json!({ "type": "apiKey", "name": "tenant key", "in": "query" })
    );
    assert!(
        OpenApiSecurityScheme::open_id_connect(
            "http://127.0.0.1:8080/.well-known/openid-configuration"
        )
        .is_ok()
    );
    assert_eq!(
        OpenApiSecurityRequirement::scheme("bearerAuth")
            .unwrap()
            .and_scheme("bearerAuth")
            .unwrap_err(),
        OpenApiError::DuplicateSecurityRequirement
    );
    assert_eq!(
        OpenApiSecurityScheme::open_id_connect("http://issuer.example/discovery").unwrap_err(),
        OpenApiError::InvalidSecuritySchemeUrl
    );
    assert_eq!(
        OpenApiSecurityScheme::open_id_connect("https://user:secret@issuer.example/discovery")
            .unwrap_err(),
        OpenApiError::InvalidSecuritySchemeUrl
    );
    assert_eq!(
        OpenApiSecurityScheme::open_id_connect("https://issuer.example/discovery?debug=1")
            .unwrap_err(),
        OpenApiError::InvalidSecuritySchemeUrl
    );
    assert_eq!(
        OpenApiSecurityScheme::open_id_connect("https://issuer.example/discovery#fragment")
            .unwrap_err(),
        OpenApiError::InvalidSecuritySchemeUrl
    );
    assert_eq!(
        OpenApiSecurityScheme::open_id_connect("https://").unwrap_err(),
        OpenApiError::InvalidSecuritySchemeUrl
    );
    assert_eq!(
        OpenApiDocument::new("Todo API", "0.1.0")
            .unwrap()
            .security_scheme("bearerAuth", OpenApiSecurityScheme::http_bearer())
            .unwrap()
            .security_scheme("bearerAuth", OpenApiSecurityScheme::http_basic())
            .unwrap_err(),
        OpenApiError::DuplicateSecurityScheme
    );
}

#[test]
fn document_renders_oauth2_flows_and_global_operation_security_semantics() {
    let authorization_code = OpenApiOAuthFlow::authorization_code(
        "https://issuer.example/authorize",
        "https://issuer.example/token",
        [("todos.read", "Read todos"), ("todos.write", "Write todos")],
    )
    .unwrap()
    .with_refresh_url("https://issuer.example/refresh")
    .unwrap();
    let client_credentials = OpenApiOAuthFlow::client_credentials(
        "https://issuer.example/token",
        [("todos.read", "Read todos")],
    )
    .unwrap();
    let document = OpenApiDocument::new("Todo API", "0.1.0")
        .unwrap()
        .security_scheme(
            "oauth",
            OpenApiSecurityScheme::oauth2([authorization_code, client_credentials]).unwrap(),
        )
        .unwrap()
        .global_security_requirement(
            OpenApiSecurityRequirement::scoped("oauth", ["todos.read"]).unwrap(),
        )
        .unwrap()
        .operation(
            OpenApiRoute::from_rustee("/todos").unwrap(),
            OpenApiMethod::Get,
            OpenApiOperation::builder("list_todos")
                .empty_response(StatusCode::OK, "Todos")
                .build()
                .unwrap(),
        )
        .unwrap()
        .operation(
            OpenApiRoute::from_rustee("/session").unwrap(),
            OpenApiMethod::Post,
            OpenApiOperation::builder("start_session")
                .clear_security_requirements()
                .empty_response(StatusCode::NO_CONTENT, "Session started")
                .build()
                .unwrap(),
        )
        .unwrap()
        .operation(
            OpenApiRoute::from_rustee("/invite").unwrap(),
            OpenApiMethod::Get,
            OpenApiOperation::builder("read_invite")
                .anonymous_access()
                .security_requirement(
                    OpenApiSecurityRequirement::scoped("oauth", ["todos.read"]).unwrap(),
                )
                .empty_response(StatusCode::OK, "Invite")
                .build()
                .unwrap(),
        )
        .unwrap()
        .to_value();

    assert_eq!(
        document["components"]["securitySchemes"]["oauth"],
        json!({
            "type": "oauth2",
            "flows": {
                "authorizationCode": {
                    "authorizationUrl": "https://issuer.example/authorize",
                    "tokenUrl": "https://issuer.example/token",
                    "refreshUrl": "https://issuer.example/refresh",
                    "scopes": {
                        "todos.read": "Read todos",
                        "todos.write": "Write todos",
                    },
                },
                "clientCredentials": {
                    "tokenUrl": "https://issuer.example/token",
                    "scopes": { "todos.read": "Read todos" },
                },
            },
        })
    );
    assert_eq!(document["security"], json!([{ "oauth": ["todos.read"] }]));
    assert!(document["paths"]["/todos"]["get"].get("security").is_none());
    assert_eq!(document["paths"]["/session"]["post"]["security"], json!([]));
    assert_eq!(
        document["paths"]["/invite"]["get"]["security"],
        json!([{}, { "oauth": ["todos.read"] }])
    );
}

#[test]
fn oauth2_security_metadata_rejects_unsafe_or_incompatible_flow_details() {
    assert_eq!(
        OpenApiSecurityScheme::oauth2(std::iter::empty::<OpenApiOAuthFlow>()).unwrap_err(),
        OpenApiError::MissingOAuthFlow
    );
    let flow = OpenApiOAuthFlow::client_credentials(
        "https://issuer.example/token",
        [("todos.read", "Read todos")],
    )
    .unwrap();
    assert_eq!(
        OpenApiSecurityScheme::oauth2([flow.clone(), flow.clone()]).unwrap_err(),
        OpenApiError::DuplicateOAuthFlow
    );
    assert_eq!(
        OpenApiOAuthFlow::client_credentials(
            "https://issuer.example/token",
            [("todos.read", "Read todos"), ("todos.read", "Read again")],
        )
        .unwrap_err(),
        OpenApiError::DuplicateOAuthScope
    );
    assert_eq!(
        OpenApiOAuthFlow::authorization_code(
            "http://issuer.example/authorize",
            "https://issuer.example/token",
            [("todos.read", "Read todos")],
        )
        .unwrap_err(),
        OpenApiError::InvalidSecuritySchemeUrl
    );

    let operation = OpenApiOperation::builder("list_todos")
        .security_requirement(OpenApiSecurityRequirement::scoped("oauth", ["todos.write"]).unwrap())
        .empty_response(StatusCode::OK, "Todos")
        .build()
        .unwrap();
    assert_eq!(
        OpenApiDocument::new("Todo API", "0.1.0")
            .unwrap()
            .security_scheme("oauth", OpenApiSecurityScheme::oauth2([flow]).unwrap())
            .unwrap()
            .operation(
                OpenApiRoute::from_rustee("/todos").unwrap(),
                OpenApiMethod::Get,
                operation,
            )
            .unwrap_err(),
        OpenApiError::UnknownOAuthScope
    );
}
