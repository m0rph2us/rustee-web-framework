//! Batch configuration, identifier, and diagnostic regression coverage.

use super::super::*;

#[test]
fn batch_decoder_preserves_only_safe_status_counts_and_file_identifiers() {
    let snapshot = decode_batch(&json!({
        "id":"batch_1",
        "status":"completed",
        "request_counts":{"total":10,"completed":8,"failed":2},
        "output_file_id":"file_output_1",
        "error_file_id":"file_error_1"
    }))
    .unwrap();
    assert_eq!(snapshot.status(), OpenAiBatchStatus::Completed);
    assert_eq!(snapshot.completed_requests(), 8);
    assert_eq!(snapshot.output_file_id(), Some("file_output_1"));
    assert!(
        decode_batch(&json!({
            "id":"batch_1",
            "status":"unknown-state",
            "request_counts":{"total":1,"completed":0,"failed":0}
        }))
        .is_err()
    );
    assert!(
        decode_batch(&json!({
            "id":"batch_1",
            "status":"completed",
            "request_counts":{"total":1,"completed":1,"failed":1}
        }))
        .is_err()
    );
    assert!(
        decode_batch(&json!({
            "id":"batch_1",
            "status":"completed",
            "request_counts":{"total":OPENAI_BATCH_MAX_REQUESTS + 1,"completed":0,"failed":0}
        }))
        .is_err()
    );
    for dot_segment in [".", ".."] {
        assert!(
            decode_batch(&json!({
                "id":dot_segment,
                "status":"completed",
                "request_counts":{"total":1,"completed":1,"failed":0}
            }))
            .is_err()
        );
        assert!(
            decode_batch(&json!({
                "id":"batch_1",
                "status":"completed",
                "request_counts":{"total":1,"completed":1,"failed":0},
                "output_file_id":dot_segment
            }))
            .is_err()
        );
        assert!(!valid_provider_path_identifier(dot_segment));
    }
}

#[test]
fn batch_identifier_debug_is_content_free() {
    let input_file = decode_batch_input_file(&json!({
        "id":"private_input_file_id",
        "purpose":"batch"
    }))
    .unwrap();
    let request =
        OpenAiBatchRequest::from_uploaded_input(&input_file, OpenAiBatchEndpoint::Responses);
    let input_row = OpenAiBatchInputRow::new(
        "private_custom_id",
        OpenAiBatchEndpoint::Responses,
        json!({"input":"private prompt"}),
    )
    .unwrap();
    let snapshot = decode_batch(&json!({
        "id":"private_batch_id",
        "status":"completed",
        "request_counts":{"total":1,"completed":1,"failed":0},
        "output_file_id":"private_output_file_id",
        "error_file_id":"private_error_file_id"
    }))
    .unwrap();
    let successful_content = OpenAiBatchFileContent::from_bytes(
        concat!(
            "{\"custom_id\":\"private_output_custom_id\",\"response\":{\"status_code\":200,",
            "\"request_id\":\"private_request_id\",\"body\":{}},\"error\":null}\n"
        )
        .as_bytes()
        .to_vec(),
    );
    let successful_row = successful_content.output_rows().next().unwrap().unwrap();
    let successful_row_debug = format!("{successful_row:?}");
    let response = match successful_row.into_outcome() {
        OpenAiBatchRowOutcome::Response(response) => response,
        OpenAiBatchRowOutcome::Error(_) => panic!("expected successful provider row"),
    };
    let failed_content = OpenAiBatchFileContent::from_bytes(
        b"{\"custom_id\":\"private_error_custom_id\",\"response\":null,\"error\":{\"code\":\"private_error_code\"}}\n".to_vec(),
    );
    let failed_row = failed_content.output_rows().next().unwrap().unwrap();
    let error = match failed_row.into_outcome() {
        OpenAiBatchRowOutcome::Response(_) => panic!("expected failed provider row"),
        OpenAiBatchRowOutcome::Error(error) => error,
    };
    let debug = format!(
        "{input_file:?}{request:?}{input_row:?}{snapshot:?}{successful_row_debug}{response:?}{error:?}"
    );

    for value in [
        "private_input_file_id",
        "private_custom_id",
        "private_batch_id",
        "private_output_file_id",
        "private_error_file_id",
        "private_output_custom_id",
        "private_request_id",
        "private_error_custom_id",
        "private_error_code",
        "private prompt",
    ] {
        assert!(
            !debug.contains(value),
            "Debug output must not include {value:?}"
        );
    }
}

#[test]
fn batch_output_expiration_is_whole_second_and_provider_bounded() {
    assert_eq!(
        OpenAiBatchOutputExpiration::new(Duration::from_secs(59 * 60)).unwrap_err(),
        OpenAiBatchOutputExpirationError::InvalidDuration
    );
    assert_eq!(
        OpenAiBatchOutputExpiration::new(Duration::from_secs(31 * 24 * 60 * 60)).unwrap_err(),
        OpenAiBatchOutputExpirationError::InvalidDuration
    );
    assert_eq!(
        OpenAiBatchOutputExpiration::new(Duration::from_secs(60 * 60) + Duration::from_nanos(1))
            .unwrap_err(),
        OpenAiBatchOutputExpirationError::InvalidDuration
    );
    let expiration = OpenAiBatchOutputExpiration::new(Duration::from_secs(24 * 60 * 60)).unwrap();
    let request = OpenAiBatchRequest::new("file_input_1", OpenAiBatchEndpoint::Responses)
        .unwrap()
        .with_output_expiration(expiration);
    assert_eq!(request.output_expiration(), Some(expiration));
}
