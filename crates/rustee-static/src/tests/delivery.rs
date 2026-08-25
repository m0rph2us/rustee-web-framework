use super::*;

#[tokio::test]
async fn serves_get_and_head_with_explicit_safe_headers() {
    let root = TempRoot::new();
    fs::write(root.path().join("app.css"), "body { color: black; }").unwrap();
    let service = StaticFiles::new(root.path())
        .unwrap()
        .at("/assets")
        .unwrap()
        .layer()
        .layer(fallback_service());

    let response = service
        .clone()
        .oneshot(request(Method::GET, "/assets/app.css"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[CONTENT_TYPE], "text/css; charset=utf-8");
    assert_eq!(response.headers()["content-length"], "22");
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert_eq!(response.headers()["x-content-type-options"], "nosniff");
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "body { color: black; }"
    );

    let response = service
        .oneshot(request(Method::HEAD, "/assets/app.css"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-length"], "22");
    assert!(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .is_empty()
    );
}

#[tokio::test]
async fn streams_large_full_and_single_range_representations_in_bounded_chunks() {
    let root = TempRoot::new();
    let asset = vec![b'x'; STREAMING_CHUNK_BYTES * 2 + 97];
    fs::write(root.path().join("large.txt"), &asset).unwrap();
    let service = StaticFiles::new(root.path())
        .unwrap()
        .at("/assets")
        .unwrap()
        .with_max_file_bytes(u64::try_from(asset.len()).unwrap())
        .unwrap()
        .with_streaming_threshold(1_024)
        .unwrap()
        .layer()
        .layer(fallback_service());

    let response = service
        .clone()
        .oneshot(request(Method::GET, "/assets/large.txt"))
        .await
        .unwrap();
    assert_eq!(response.headers()[CONTENT_LENGTH], asset.len().to_string());
    let (chunk_sizes, body) = collect_data_chunks(response.into_body()).await;
    assert!(chunk_sizes.len() >= 2);
    assert!(
        chunk_sizes
            .iter()
            .all(|size| *size <= STREAMING_CHUNK_BYTES)
    );
    assert_eq!(body, asset);

    let mut range = request(Method::GET, "/assets/large.txt");
    range.headers_mut().insert(
        RANGE,
        format!("bytes=512-{}", asset.len() - 513).parse().unwrap(),
    );
    let response = service.oneshot(range).await.unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        response.headers()[CONTENT_LENGTH],
        (asset.len() - 1_024).to_string()
    );
    let (chunk_sizes, body) = collect_data_chunks(response.into_body()).await;
    assert!(chunk_sizes.len() >= 2);
    assert!(
        chunk_sizes
            .iter()
            .all(|size| *size <= STREAMING_CHUNK_BYTES)
    );
    assert_eq!(body, asset[512..asset.len() - 512]);
}

#[tokio::test]
async fn confines_the_mount_and_rejects_decoded_traversal() {
    let root = TempRoot::new();
    fs::write(root.path().join("visible.txt"), "visible").unwrap();
    let secret = root
        .path()
        .parent()
        .unwrap()
        .join("rustee-static-secret.txt");
    fs::write(&secret, "secret").unwrap();
    let service = StaticFiles::new(root.path())
        .unwrap()
        .at("/assets")
        .unwrap()
        .with_max_file_bytes(3)
        .unwrap()
        .layer()
        .layer(fallback_service());

    assert_eq!(
        service
            .clone()
            .oneshot(request(Method::GET, "/application"))
            .await
            .unwrap()
            .status(),
        StatusCode::IM_A_TEAPOT
    );
    assert_eq!(
        service
            .clone()
            .oneshot(request(Method::GET, "/assets-other/visible.txt"))
            .await
            .unwrap()
            .status(),
        StatusCode::IM_A_TEAPOT
    );
    assert_eq!(
        service
            .clone()
            .oneshot(request(Method::POST, "/assets/visible.txt"))
            .await
            .unwrap()
            .status(),
        StatusCode::IM_A_TEAPOT
    );
    assert_eq!(
        service
            .oneshot(request(
                Method::GET,
                "/assets/%2e%2e/rustee-static-secret.txt"
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        StaticFiles::new(root.path())
            .unwrap()
            .at("/assets")
            .unwrap()
            .with_max_file_bytes(3)
            .unwrap()
            .layer()
            .layer(fallback_service())
            .oneshot(request(Method::GET, "/assets/visible.txt"))
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    let _ = fs::remove_file(secret);
}
