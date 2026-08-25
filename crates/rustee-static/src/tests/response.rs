use super::*;

#[tokio::test]
async fn conditional_requests_use_weak_validators_and_precedence() {
    let root = TempRoot::new();
    fs::write(root.path().join("app.css"), "body { color: black; }").unwrap();
    let service = StaticFiles::new(root.path())
        .unwrap()
        .at("/assets")
        .unwrap()
        .with_cache_control("public, max-age=60".parse().unwrap())
        .layer()
        .layer(fallback_service());

    let initial = service
        .clone()
        .oneshot(request(Method::GET, "/assets/app.css"))
        .await
        .unwrap();
    let etag = initial.headers()[ETAG].clone();
    let last_modified = initial.headers()[LAST_MODIFIED].clone();
    assert!(etag.to_str().unwrap().starts_with("W/\""));
    assert_eq!(
        initial.into_body().collect().await.unwrap().to_bytes(),
        "body { color: black; }"
    );

    let mut if_none_match = request(Method::GET, "/assets/app.css");
    if_none_match
        .headers_mut()
        .insert(IF_NONE_MATCH, etag.clone());
    let response = service.clone().oneshot(if_none_match).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
    assert_eq!(response.headers()[ETAG], etag);
    assert_eq!(response.headers()[LAST_MODIFIED], last_modified);
    assert_eq!(response.headers()[CACHE_CONTROL], "public, max-age=60");
    assert!(response.headers().get(CONTENT_TYPE).is_none());
    assert!(response.headers().get(CONTENT_LENGTH).is_none());
    assert!(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .is_empty()
    );

    let mut weak_comparison = request(Method::HEAD, "/assets/app.css");
    weak_comparison.headers_mut().insert(
        IF_NONE_MATCH,
        etag.to_str()
            .unwrap()
            .strip_prefix("W/")
            .unwrap()
            .parse()
            .unwrap(),
    );
    assert_eq!(
        service
            .clone()
            .oneshot(weak_comparison)
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_MODIFIED
    );

    let mut if_modified_since = request(Method::GET, "/assets/app.css");
    if_modified_since
        .headers_mut()
        .insert(IF_MODIFIED_SINCE, last_modified.clone());
    assert_eq!(
        service
            .clone()
            .oneshot(if_modified_since)
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_MODIFIED
    );

    let mut etag_precedence = request(Method::GET, "/assets/app.css");
    etag_precedence
        .headers_mut()
        .insert(IF_NONE_MATCH, "W/\"not-this-version\"".parse().unwrap());
    etag_precedence
        .headers_mut()
        .insert(IF_MODIFIED_SINCE, last_modified);
    let response = service.oneshot(etag_precedence).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "body { color: black; }"
    );
}
