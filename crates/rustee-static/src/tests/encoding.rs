use super::*;

#[tokio::test]
async fn streams_a_selected_precompressed_variant() {
    let root = TempRoot::new();
    let identity = b"identity asset";
    let variant = vec![b'b'; STREAMING_CHUNK_BYTES + 23];
    fs::write(root.path().join("app.js"), identity).unwrap();
    fs::write(root.path().join("app.js.br"), &variant).unwrap();
    let service = StaticFiles::new(root.path())
        .unwrap()
        .at("/assets")
        .unwrap()
        .with_max_file_bytes(u64::try_from(variant.len()).unwrap())
        .unwrap()
        .with_streaming_threshold(1_024)
        .unwrap()
        .with_precompressed_variants(true)
        .layer()
        .layer(fallback_service());

    let response = service.oneshot(precompressed_request("br")).await.unwrap();
    assert_eq!(response.headers()[CONTENT_ENCODING], "br");
    assert_eq!(
        response.headers()[CONTENT_LENGTH],
        variant.len().to_string()
    );
    let (chunk_sizes, body) = collect_data_chunks(response.into_body()).await;
    assert!(chunk_sizes.len() >= 2);
    assert!(
        chunk_sizes
            .iter()
            .all(|size| *size <= STREAMING_CHUNK_BYTES)
    );
    assert_eq!(body, variant);
}

#[tokio::test]
async fn precompressed_variants_are_opt_in_and_negotiate_content_coding() {
    let root = TempRoot::new();
    fs::write(root.path().join("app.js"), "identity asset").unwrap();
    fs::write(root.path().join("app.js.br"), "brotli asset").unwrap();
    fs::write(root.path().join("app.js.gz"), "gzip asset").unwrap();

    let identity = StaticFiles::new(root.path())
        .unwrap()
        .at("/assets")
        .unwrap()
        .layer()
        .layer(fallback_service());
    let response = identity
        .oneshot(precompressed_request("br, gzip"))
        .await
        .unwrap();
    assert!(response.headers().get(CONTENT_ENCODING).is_none());
    assert!(response.headers().get(VARY).is_none());
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "identity asset"
    );

    let service = StaticFiles::new(root.path())
        .unwrap()
        .at("/assets")
        .unwrap()
        .with_precompressed_variants(true)
        .layer()
        .layer(fallback_service());
    let response = service
        .clone()
        .oneshot(precompressed_request("br;q=0.9, gzip"))
        .await
        .unwrap();
    assert_eq!(response.headers()[CONTENT_ENCODING], "gzip");
    assert_eq!(response.headers()[VARY], "Accept-Encoding");
    assert_eq!(
        response.headers()[CONTENT_TYPE],
        "text/javascript; charset=utf-8"
    );
    assert_eq!(response.headers()[CONTENT_LENGTH], "10");
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "gzip asset"
    );

    fs::remove_file(root.path().join("app.js.br")).unwrap();
    let response = service
        .clone()
        .oneshot(precompressed_request("br, gzip;q=0.5"))
        .await
        .unwrap();
    assert_eq!(response.headers()[CONTENT_ENCODING], "gzip");
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "gzip asset"
    );

    let response = service
        .oneshot(precompressed_request("identity"))
        .await
        .unwrap();
    assert!(response.headers().get(CONTENT_ENCODING).is_none());
    assert_eq!(response.headers()[VARY], "Accept-Encoding");
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "identity asset"
    );
}

#[tokio::test]
async fn precompressed_variants_keep_variant_validators_and_range_identity() {
    let root = TempRoot::new();
    fs::write(root.path().join("app.js"), "identity asset").unwrap();
    fs::write(root.path().join("app.js.br"), "brotli asset").unwrap();
    let service = StaticFiles::new(root.path())
        .unwrap()
        .at("/assets")
        .unwrap()
        .with_precompressed_variants(true)
        .layer()
        .layer(fallback_service());

    let initial = service
        .clone()
        .oneshot(precompressed_request("br"))
        .await
        .unwrap();
    let etag = initial.headers()[ETAG].clone();
    assert_eq!(initial.headers()[CONTENT_ENCODING], "br");

    let mut not_modified = precompressed_request("br");
    not_modified.headers_mut().insert(IF_NONE_MATCH, etag);
    let response = service.clone().oneshot(not_modified).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
    assert_eq!(response.headers()[CONTENT_ENCODING], "br");
    assert_eq!(response.headers()[VARY], "Accept-Encoding");

    let mut range = precompressed_request("br");
    range
        .headers_mut()
        .insert(RANGE, "bytes=0-3,5-7".parse().unwrap());
    let response = service.oneshot(range).await.unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert!(response.headers().get(CONTENT_ENCODING).is_none());
    assert!(response.headers().get(VARY).is_none());
    assert!(response.headers().get(CONTENT_RANGE).is_none());
    assert!(
        response.headers()[CONTENT_TYPE]
            .to_str()
            .unwrap()
            .starts_with("multipart/byteranges; boundary=")
    );
}
