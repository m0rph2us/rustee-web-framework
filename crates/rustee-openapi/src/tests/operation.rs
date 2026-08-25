use http::StatusCode;

use crate::{OpenApiError, OpenApiOperation, OpenApiSchema};

#[test]
fn operation_rejects_duplicate_parameters_responses_and_missing_responses() {
    assert_eq!(
        OpenApiOperation::builder("search_todos")
            .empty_response(StatusCode::OK, "Results")
            .empty_response(StatusCode::OK, "Replacement results")
            .build()
            .unwrap_err(),
        OpenApiError::DuplicateResponse
    );
    assert_eq!(
        OpenApiOperation::builder("search_todos")
            .query_parameter("cursor", false, OpenApiSchema::string())
            .query_parameter("cursor", false, OpenApiSchema::string())
            .empty_response(StatusCode::OK, "Results")
            .build()
            .unwrap_err(),
        OpenApiError::DuplicateParameter
    );
    assert_eq!(
        OpenApiOperation::builder("search_todos")
            .build()
            .unwrap_err(),
        OpenApiError::MissingResponse
    );
}

#[test]
fn operation_header_parameters_require_http_field_names_and_are_case_insensitive() {
    assert_eq!(
        OpenApiOperation::builder("get_todo")
            .header_parameter("X Trace", false, OpenApiSchema::string())
            .empty_response(StatusCode::OK, "Todo")
            .build()
            .unwrap_err(),
        OpenApiError::InvalidMetadata {
            field: "header parameter name",
        }
    );
    assert_eq!(
        OpenApiOperation::builder("get_todo")
            .header_parameter("X-Trace", false, OpenApiSchema::string())
            .header_parameter("x-trace", false, OpenApiSchema::string())
            .empty_response(StatusCode::OK, "Todo")
            .build()
            .unwrap_err(),
        OpenApiError::DuplicateParameter
    );
}
