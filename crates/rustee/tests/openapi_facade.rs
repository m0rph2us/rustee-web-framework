use rustee::{
    StatusCode,
    openapi::{
        OpenApiApiKeyLocation, OpenApiDocument, OpenApiError, OpenApiMethod, OpenApiOAuthFlow,
        OpenApiOperation, OpenApiOperationBuilder, OpenApiParameterLocation, OpenApiRoute,
        OpenApiSchema, OpenApiSecurityRequirement, OpenApiSecurityScheme,
    },
};

#[test]
fn facade_exposes_the_deliberate_openapi_surface() {
    let _: Option<OpenApiError> = None;
    let _: OpenApiOperationBuilder = OpenApiOperation::builder("facade_surface");
    let _: OpenApiParameterLocation = OpenApiParameterLocation::Query;
}

#[test]
fn documented_minimal_openapi_path_builds_through_the_facade() {
    let document = OpenApiDocument::new("Rustee API", "1.0.0")
        .unwrap()
        .security_scheme("bearerAuth", OpenApiSecurityScheme::http_bearer())
        .unwrap()
        .operation(
            OpenApiRoute::from_rustee("/users/:id").unwrap(),
            OpenApiMethod::Get,
            OpenApiOperation::builder("get_user")
                .path_parameter("id", OpenApiSchema::string())
                .security_requirement(OpenApiSecurityRequirement::scheme("bearerAuth").unwrap())
                .empty_response(StatusCode::OK, "User")
                .build()
                .unwrap(),
        )
        .unwrap();

    assert_eq!(
        document.to_value()["paths"]["/users/{id}"]["get"]["operationId"],
        "get_user"
    );
}

#[test]
fn facade_reexports_explicit_openapi_security_metadata() {
    assert!(OpenApiSecurityScheme::api_key("X-Tenant-Key", OpenApiApiKeyLocation::Header).is_ok());
    assert!(OpenApiSecurityScheme::api_key("x tenant key", OpenApiApiKeyLocation::Header).is_err());

    let document = OpenApiDocument::new("Todo API", "0.1.0")
        .unwrap()
        .security_scheme(
            "oidc",
            OpenApiSecurityScheme::open_id_connect(
                "https://issuer.example/.well-known/openid-configuration",
            )
            .unwrap(),
        )
        .unwrap()
        .security_scheme("serviceTls", OpenApiSecurityScheme::mutual_tls())
        .unwrap()
        .operation(
            OpenApiRoute::from_rustee("/todos").unwrap(),
            OpenApiMethod::Get,
            OpenApiOperation::builder("list_todos")
                .security_requirement(
                    OpenApiSecurityRequirement::scoped("oidc", ["todos.read"]).unwrap(),
                )
                .empty_response(StatusCode::OK, "Todos")
                .build()
                .unwrap(),
        )
        .unwrap()
        .to_value();

    assert_eq!(
        document["paths"]["/todos"]["get"]["security"][0]["oidc"],
        serde_json::json!(["todos.read"])
    );
    assert_eq!(
        document["components"]["securitySchemes"]["serviceTls"],
        serde_json::json!({ "type": "mutualTLS" })
    );
}

#[test]
fn facade_reexports_oauth2_and_document_security_overrides() {
    let flow = OpenApiOAuthFlow::client_credentials(
        "https://issuer.example/token",
        [("todos.read", "Read todos")],
    )
    .unwrap();
    let document = OpenApiDocument::new("Todo API", "0.1.0")
        .unwrap()
        .security_scheme("oauth", OpenApiSecurityScheme::oauth2([flow]).unwrap())
        .unwrap()
        .global_security_requirement(
            OpenApiSecurityRequirement::scoped("oauth", ["todos.read"]).unwrap(),
        )
        .unwrap()
        .operation(
            OpenApiRoute::from_rustee("/health").unwrap(),
            OpenApiMethod::Get,
            OpenApiOperation::builder("health")
                .clear_security_requirements()
                .empty_response(StatusCode::OK, "Healthy")
                .build()
                .unwrap(),
        )
        .unwrap()
        .to_value();

    assert_eq!(
        document["security"],
        serde_json::json!([{ "oauth": ["todos.read"] }])
    );
    assert_eq!(
        document["paths"]["/health"]["get"]["security"],
        serde_json::json!([])
    );
}
