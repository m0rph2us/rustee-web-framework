use super::*;

#[derive(Clone, Copy)]
struct UnavailableApiKeyAuthenticator;

impl ApiKeyAuthenticator for UnavailableApiKeyAuthenticator {
    fn authenticate(
        &self,
        _: &str,
    ) -> futures_util::future::BoxFuture<'static, Result<Principal, ApiKeyError>> {
        Box::pin(future::ready(Err(ApiKeyError::ProviderUnavailable)))
    }
}

#[derive(Clone)]
struct FingerprintStore {
    expected: ApiKeyFingerprint,
    principal: Principal,
}

impl ApiKeyFingerprintStore for FingerprintStore {
    fn authenticate(
        &self,
        fingerprint: ApiKeyFingerprint,
    ) -> futures_util::future::BoxFuture<'static, Result<Principal, ApiKeyError>> {
        let principal = (fingerprint == self.expected).then(|| self.principal.clone());
        Box::pin(future::ready(principal.ok_or(ApiKeyError::RejectedApiKey)))
    }
}

#[derive(Clone)]
struct RecordingFingerprintStore {
    accepted: ApiKeyFingerprint,
    principal: Principal,
    attempts: Arc<Mutex<Vec<ApiKeyFingerprint>>>,
    unavailable: Option<ApiKeyFingerprint>,
}

struct RetiredPepperIterator {
    yielded: usize,
}

impl Iterator for RetiredPepperIterator {
    type Item = ApiKeyPepper;

    fn next(&mut self) -> Option<Self::Item> {
        assert!(
            self.yielded <= MAX_RETIRED_API_KEY_PEPPERS,
            "pepper ring must stop reading once it rejects an oversized iterator"
        );
        self.yielded += 1;
        let byte = u8::try_from(self.yielded + 1).unwrap();
        Some(ApiKeyPepper::new([byte; 32]).unwrap())
    }
}

impl ApiKeyFingerprintStore for RecordingFingerprintStore {
    fn authenticate(
        &self,
        fingerprint: ApiKeyFingerprint,
    ) -> futures_util::future::BoxFuture<'static, Result<Principal, ApiKeyError>> {
        let accepted = self.accepted.clone();
        let principal = self.principal.clone();
        let attempts = self.attempts.clone();
        let unavailable = self.unavailable.as_ref() == Some(&fingerprint);
        Box::pin(async move {
            attempts.lock().unwrap().push(fingerprint.clone());
            if unavailable {
                return Err(ApiKeyError::ProviderUnavailable);
            }
            (fingerprint == accepted)
                .then_some(principal)
                .ok_or(ApiKeyError::RejectedApiKey)
        })
    }
}

fn api_key_authenticator() -> StaticApiKeyAuthenticator {
    let mut authenticator = StaticApiKeyAuthenticator::new();
    authenticator
        .insert(
            "local-api-key",
            Principal::new("service-client")
                .unwrap()
                .with_scope("profile:read")
                .unwrap(),
        )
        .unwrap();
    authenticator
}

fn api_key_request(values: &[&str]) -> rustee_core::Request {
    let mut request = HttpRequest::builder()
        .method("GET")
        .uri("/me")
        .body(empty_body())
        .unwrap();
    for value in values {
        request.headers_mut().append(
            "x-api-key",
            HeaderValue::from_str(value).expect("test API key header must be valid"),
        );
    }
    request
}

async fn raw_http_request(address: std::net::SocketAddr, request: &[u8]) -> String {
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream.write_all(request).await.unwrap();
    let mut response = Vec::new();
    timeout(Duration::from_secs(2), stream.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    String::from_utf8(response).unwrap()
}
#[tokio::test]
async fn api_key_layer_authenticates_one_explicit_header_without_exposing_the_key() {
    let service = ApiKeyLayer::header("X-API-Key", api_key_authenticator())
        .unwrap()
        .layer(
            App::new().get("/me", |AuthUser(principal): AuthUser| async move {
                principal.subject().to_owned()
            }),
        );

    let response = service
        .oneshot(api_key_request(&["local-api-key"]))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn keyed_api_key_authenticator_uses_only_a_keyed_fingerprint_for_lookup() {
    let pepper = ApiKeyPepper::new([7; 32]).unwrap();
    let expected = pepper.fingerprint("local-api-key").unwrap();
    let authenticator = KeyedApiKeyAuthenticator::new(
        pepper,
        FingerprintStore {
            expected,
            principal: Principal::new("service-client").unwrap(),
        },
    );
    let service = ApiKeyLayer::header("x-api-key", authenticator)
        .unwrap()
        .layer(
            App::new().get("/me", |AuthUser(principal): AuthUser| async move {
                principal.subject().to_owned()
            }),
        );

    let response = service
        .oneshot(api_key_request(&["local-api-key"]))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn rotating_keyed_api_key_authenticator_tries_a_retired_pepper_only_after_a_rejection() {
    let active = ApiKeyPepper::new([8; 32]).unwrap();
    let retired = ApiKeyPepper::new([7; 32]).unwrap();
    let active_fingerprint = active.fingerprint("local-api-key").unwrap();
    let retired_fingerprint = retired.fingerprint("local-api-key").unwrap();
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let principal = Principal::new("service-client").unwrap();
    let authenticator = RotatingKeyedApiKeyAuthenticator::new(
        ApiKeyPepperRing::with_retired(active, [retired]).unwrap(),
        RecordingFingerprintStore {
            accepted: retired_fingerprint.clone(),
            principal: principal.clone(),
            attempts: attempts.clone(),
            unavailable: None,
        },
    );

    assert_eq!(
        authenticator.authenticate("local-api-key").await.unwrap(),
        principal
    );
    assert_eq!(
        *attempts.lock().unwrap(),
        vec![active_fingerprint, retired_fingerprint]
    );
}

#[tokio::test]
async fn rotating_keyed_api_key_authenticator_fails_closed_without_trying_retired_peppers() {
    let active = ApiKeyPepper::new([8; 32]).unwrap();
    let retired = ApiKeyPepper::new([7; 32]).unwrap();
    let active_fingerprint = active.fingerprint("local-api-key").unwrap();
    let retired_fingerprint = retired.fingerprint("local-api-key").unwrap();
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let authenticator = RotatingKeyedApiKeyAuthenticator::new(
        ApiKeyPepperRing::with_retired(active, [retired]).unwrap(),
        RecordingFingerprintStore {
            accepted: retired_fingerprint,
            principal: Principal::new("service-client").unwrap(),
            attempts: attempts.clone(),
            unavailable: Some(active_fingerprint.clone()),
        },
    );

    assert_eq!(
        authenticator
            .authenticate("local-api-key")
            .await
            .unwrap_err(),
        ApiKeyError::ProviderUnavailable
    );
    assert_eq!(*attempts.lock().unwrap(), vec![active_fingerprint]);
}

#[tokio::test]
async fn rotating_keyed_api_key_authenticator_rejects_an_invalid_key_before_store_lookup() {
    let active = ApiKeyPepper::new([8; 32]).unwrap();
    let expected = active.fingerprint("local-api-key").unwrap();
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let authenticator = RotatingKeyedApiKeyAuthenticator::new(
        ApiKeyPepperRing::new(active),
        RecordingFingerprintStore {
            accepted: expected,
            principal: Principal::new("service-client").unwrap(),
            attempts: attempts.clone(),
            unavailable: None,
        },
    );

    assert_eq!(
        authenticator
            .authenticate("not a valid key")
            .await
            .unwrap_err(),
        ApiKeyError::InvalidApiKey
    );
    assert!(attempts.lock().unwrap().is_empty());
}

#[test]
fn api_key_pepper_fingerprint_is_stable_bounded_and_redacted() {
    let pepper = ApiKeyPepper::new([7; 32]).unwrap();
    let fingerprint = pepper.fingerprint("local-api-key").unwrap();
    assert_eq!(fingerprint, pepper.fingerprint("local-api-key").unwrap());
    assert_ne!(fingerprint, pepper.fingerprint("other-api-key").unwrap());
    assert_eq!(fingerprint.as_bytes().len(), 32);
    assert_eq!(format!("{pepper:?}"), "ApiKeyPepper([redacted])");
    assert_eq!(format!("{fingerprint:?}"), "ApiKeyFingerprint([redacted])");
    assert_eq!(
        pepper.fingerprint("not a valid key").unwrap_err(),
        ApiKeyError::InvalidApiKey
    );
    assert_eq!(
        ApiKeyPepper::new([0; 32]).unwrap_err(),
        ApiKeyPepperError::AllZero
    );
}

#[test]
fn api_key_pepper_ring_is_bounded_distinct_and_redacted() {
    let active = ApiKeyPepper::new([1; 32]).unwrap();
    let retired = ApiKeyPepper::new([2; 32]).unwrap();
    let ring = ApiKeyPepperRing::with_retired(active.clone(), [retired]).unwrap();
    let rendered = format!("{ring:?}");
    assert!(rendered.contains("retired_pepper_count: 1"));
    assert!(!rendered.contains("[1, 1"));
    assert!(matches!(
        ApiKeyPepperRing::with_retired(active.clone(), [active.clone()]),
        Err(ApiKeyPepperRingError::DuplicatePepper)
    ));
    assert!(matches!(
        ApiKeyPepperRing::with_retired(
            active,
            [
                ApiKeyPepper::new([2; 32]).unwrap(),
                ApiKeyPepper::new([3; 32]).unwrap(),
                ApiKeyPepper::new([4; 32]).unwrap(),
            ],
        ),
        Err(ApiKeyPepperRingError::TooManyRetired)
    ));
    assert!(matches!(
        ApiKeyPepperRing::with_retired(
            ApiKeyPepper::new([1; 32]).unwrap(),
            RetiredPepperIterator { yielded: 0 },
        ),
        Err(ApiKeyPepperRingError::TooManyRetired)
    ));
    assert_eq!(MAX_RETIRED_API_KEY_PEPPERS, 2);
}

#[tokio::test]
async fn api_key_layer_rejects_missing_duplicate_or_malformed_values() {
    let service = ApiKeyLayer::header("x-api-key", api_key_authenticator())
        .unwrap()
        .layer(App::new().get("/me", || async { "unexpected" }));

    let missing = service.clone().oneshot(api_key_request(&[])).await.unwrap();
    assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(missing.headers()[WWW_AUTHENTICATE], "ApiKey");

    let duplicate = service
        .clone()
        .oneshot(api_key_request(&["local-api-key", "other-key"]))
        .await
        .unwrap();
    assert_eq!(duplicate.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(duplicate.headers()[WWW_AUTHENTICATE], "ApiKey");

    let malformed = service
        .oneshot(api_key_request(&["local api key"]))
        .await
        .unwrap();
    assert_eq!(malformed.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(malformed.headers()[WWW_AUTHENTICATE], "ApiKey");
}

#[tokio::test]
async fn api_key_layer_rejects_duplicate_headers_over_real_tcp_without_echoing_them() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let service = ApiKeyLayer::header("x-api-key", api_key_authenticator())
        .unwrap()
        .layer(App::new().get("/me", || async { "unexpected" }));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        serve_service_listener_with_options(
            listener,
            service,
            ServerOptions::default(),
            async move {
                let _ = shutdown_rx.await;
            },
        )
        .await
        .unwrap();
    });

    let response = raw_http_request(
        address,
        b"GET /me HTTP/1.1\r\nHost: localhost\r\nX-API-Key: local-api-key\r\nX-API-Key: duplicate-api-key\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 401 Unauthorized\r\n"));
    assert!(response.contains("www-authenticate: ApiKey\r\n"));
    assert!(response.contains("\"code\":\"invalid_api_key\""));
    assert!(!response.contains("local-api-key"));
    assert!(!response.contains("duplicate-api-key"));

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn api_key_layer_returns_a_sanitized_503_when_a_provider_is_unavailable() {
    let service = ApiKeyLayer::header("x-api-key", UnavailableApiKeyAuthenticator)
        .unwrap()
        .layer(App::new().get("/me", || async { "unexpected" }));

    let response = service
        .oneshot(api_key_request(&["local-api-key"]))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(response.headers().get(WWW_AUTHENTICATE).is_none());
}

#[test]
fn api_key_configuration_is_validated_without_logging_credentials() {
    assert_eq!(
        ApiKeyLayer::header("x api key", api_key_authenticator()).unwrap_err(),
        ApiKeyLayerError::InvalidHeaderName
    );
    assert_eq!(
        StaticApiKeyAuthenticator::new()
            .insert("contains space", Principal::new("service-client").unwrap())
            .unwrap_err(),
        StaticApiKeyError::InvalidKey
    );
    assert_eq!(
        format!("{:?}", api_key_authenticator()),
        "StaticApiKeyAuthenticator { registered_keys: 1 }"
    );
}

#[test]
fn static_api_key_authenticator_rejects_duplicate_keys() {
    let mut authenticator = StaticApiKeyAuthenticator::new();
    authenticator
        .insert("local-api-key", Principal::new("first").unwrap())
        .unwrap();

    assert_eq!(
        authenticator
            .insert("local-api-key", Principal::new("replacement").unwrap())
            .unwrap_err(),
        StaticApiKeyError::DuplicateKey
    );
}
