use super::*;

#[tokio::test]
async fn single_byte_ranges_preserve_static_response_headers() {
    let root = TempRoot::new();
    fs::write(root.path().join("sequence.txt"), "0123456789").unwrap();
    let service = StaticFiles::new(root.path())
        .unwrap()
        .at("/assets")
        .unwrap()
        .with_cache_control("public, max-age=60".parse().unwrap())
        .layer()
        .layer(fallback_service());

    let mut partial = request(Method::GET, "/assets/sequence.txt");
    partial
        .headers_mut()
        .insert(RANGE, "bytes=2-5".parse().unwrap());
    let response = service.clone().oneshot(partial).await.unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(response.headers()[ACCEPT_RANGES], "bytes");
    assert_eq!(response.headers()[CONTENT_RANGE], "bytes 2-5/10");
    assert_eq!(response.headers()[CONTENT_LENGTH], "4");
    assert_eq!(response.headers()[CACHE_CONTROL], "public, max-age=60");
    assert!(response.headers().contains_key(ETAG));
    assert!(response.headers().contains_key(LAST_MODIFIED));
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "2345"
    );

    let mut suffix_head = request(Method::HEAD, "/assets/sequence.txt");
    suffix_head
        .headers_mut()
        .insert(RANGE, "bytes=-3".parse().unwrap());
    let response = service.clone().oneshot(suffix_head).await.unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(response.headers()[CONTENT_RANGE], "bytes 7-9/10");
    assert_eq!(response.headers()[CONTENT_LENGTH], "3");
    assert!(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .is_empty()
    );

    let mut bounded_end = request(Method::GET, "/assets/sequence.txt");
    bounded_end
        .headers_mut()
        .insert(RANGE, "bytes=8-100".parse().unwrap());
    let response = service.clone().oneshot(bounded_end).await.unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(response.headers()[CONTENT_RANGE], "bytes 8-9/10");
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "89"
    );

    let mut unsatisfiable = request(Method::GET, "/assets/sequence.txt");
    unsatisfiable
        .headers_mut()
        .insert(RANGE, "bytes=50-".parse().unwrap());
    let response = service.clone().oneshot(unsatisfiable).await.unwrap();
    assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(response.headers()[CONTENT_RANGE], "bytes */10");
    assert!(response.headers().get(CONTENT_LENGTH).is_none());
}

#[tokio::test]
async fn multipart_ranges_normalize_and_keep_their_header_contract() {
    let root = TempRoot::new();
    fs::write(root.path().join("sequence.txt"), "0123456789").unwrap();
    let service = StaticFiles::new(root.path())
        .unwrap()
        .at("/assets")
        .unwrap()
        .layer()
        .layer(fallback_service());

    let mut multiple = request(Method::GET, "/assets/sequence.txt");
    multiple
        .headers_mut()
        .insert(RANGE, "bytes=8-9,0-1,1-3".parse().unwrap());
    let response = service.clone().oneshot(multiple).await.unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert!(response.headers().get(CONTENT_RANGE).is_none());
    let multipart_content_type = response.headers()[CONTENT_TYPE].to_str().unwrap();
    let boundary = multipart_content_type
        .strip_prefix("multipart/byteranges; boundary=")
        .unwrap()
        .to_owned();
    let content_length: usize = response.headers()[CONTENT_LENGTH]
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.len(), content_length);
    assert_eq!(
        body,
        format!(
            "--{boundary}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Range: bytes 0-3/10\r\n\r\n0123\r\n--{boundary}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Range: bytes 8-9/10\r\n\r\n89\r\n--{boundary}--\r\n"
        )
    );

    let mut multiple_head = request(Method::HEAD, "/assets/sequence.txt");
    multiple_head
        .headers_mut()
        .insert(RANGE, "bytes=0-1,8-9".parse().unwrap());
    let response = service.oneshot(multiple_head).await.unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert!(
        response.headers()[CONTENT_TYPE]
            .to_str()
            .unwrap()
            .starts_with("multipart/byteranges; boundary=")
    );
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
async fn malformed_or_excessive_range_sets_are_unsatisfiable() {
    let root = TempRoot::new();
    fs::write(root.path().join("sequence.txt"), "0123456789").unwrap();
    let service = StaticFiles::new(root.path())
        .unwrap()
        .at("/assets")
        .unwrap()
        .layer()
        .layer(fallback_service());

    let mut malformed = request(Method::GET, "/assets/sequence.txt");
    malformed
        .headers_mut()
        .insert(RANGE, "bytes=0-1,not-a-range".parse().unwrap());
    let response = service.clone().oneshot(malformed).await.unwrap();
    assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(response.headers()[CONTENT_RANGE], "bytes */10");

    let ranges = (0..=MAX_RANGE_MEMBERS)
        .map(|index| format!("{index}-{index}"))
        .collect::<Vec<_>>()
        .join(",");
    let mut excessive = request(Method::GET, "/assets/sequence.txt");
    excessive
        .headers_mut()
        .insert(RANGE, ranges.parse().unwrap());
    assert_eq!(
        service.oneshot(excessive).await.unwrap().status(),
        StatusCode::RANGE_NOT_SATISFIABLE
    );
}

#[tokio::test]
async fn unsupported_range_units_are_ignored_while_byte_units_are_case_insensitive() {
    let root = TempRoot::new();
    fs::write(root.path().join("sequence.txt"), "0123456789").unwrap();
    let service = StaticFiles::new(root.path())
        .unwrap()
        .at("/assets")
        .unwrap()
        .layer()
        .layer(fallback_service());

    let mut unsupported = request(Method::GET, "/assets/sequence.txt");
    unsupported
        .headers_mut()
        .insert(RANGE, "items=0-1".parse().unwrap());
    let response = service.clone().oneshot(unsupported).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().get(CONTENT_RANGE).is_none());
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "0123456789"
    );

    let mut upper_case_bytes = request(Method::GET, "/assets/sequence.txt");
    upper_case_bytes
        .headers_mut()
        .insert(RANGE, "BYTES=2-4".parse().unwrap());
    let response = service.oneshot(upper_case_bytes).await.unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(response.headers()[CONTENT_RANGE], "bytes 2-4/10");
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "234"
    );
}

#[tokio::test]
async fn range_conditions_prefer_not_modified_and_require_date_if_range() {
    let root = TempRoot::new();
    fs::write(root.path().join("sequence.txt"), "0123456789").unwrap();
    let service = StaticFiles::new(root.path())
        .unwrap()
        .at("/assets")
        .unwrap()
        .layer()
        .layer(fallback_service());

    let initial = service
        .clone()
        .oneshot(request(Method::GET, "/assets/sequence.txt"))
        .await
        .unwrap();
    let etag = initial.headers()[ETAG].clone();
    let last_modified = initial.headers()[LAST_MODIFIED].clone();

    let mut date_match = request(Method::GET, "/assets/sequence.txt");
    date_match
        .headers_mut()
        .insert(RANGE, "bytes=0-1,3-4".parse().unwrap());
    date_match
        .headers_mut()
        .insert(IF_RANGE, last_modified.clone());
    let response = service.clone().oneshot(date_match).await.unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert!(
        response.headers()[CONTENT_TYPE]
            .to_str()
            .unwrap()
            .starts_with("multipart/byteranges; boundary=")
    );

    let after_last_modified = httpdate::parse_http_date(
        last_modified
            .to_str()
            .expect("static last-modified value is ASCII"),
    )
    .expect("static last-modified value is an HTTP date")
    .checked_add(Duration::from_secs(1))
    .expect("test date remains representable");
    let mut date_after_match = request(Method::GET, "/assets/sequence.txt");
    date_after_match
        .headers_mut()
        .insert(RANGE, "bytes=5-6".parse().unwrap());
    date_after_match.headers_mut().insert(
        IF_RANGE,
        httpdate::fmt_http_date(after_last_modified)
            .parse()
            .expect("formatted HTTP date is a valid header"),
    );
    let response = service.clone().oneshot(date_after_match).await.unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "56"
    );

    let mut weak_etag = request(Method::GET, "/assets/sequence.txt");
    weak_etag
        .headers_mut()
        .insert(RANGE, "bytes=0-1,3-4".parse().unwrap());
    weak_etag.headers_mut().insert(IF_RANGE, etag.clone());
    let response = service.clone().oneshot(weak_etag).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "0123456789"
    );

    let mut not_modified = request(Method::GET, "/assets/sequence.txt");
    not_modified
        .headers_mut()
        .insert(RANGE, "bytes=0-1,3-4".parse().unwrap());
    not_modified.headers_mut().insert(IF_NONE_MATCH, etag);
    assert_eq!(
        service.oneshot(not_modified).await.unwrap().status(),
        StatusCode::NOT_MODIFIED
    );
}
