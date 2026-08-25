use std::collections::BTreeMap;

use http::{StatusCode, header::CONTENT_TYPE};
use http_body_util::BodyExt;
use rustee_core::IntoResponse;
use serde_json::Value;

use crate::{
    OpenApiDocument, OpenApiError, OpenApiMethod, OpenApiOperation, OpenApiRoute, OpenApiSchema,
};

fn todo_schema() -> OpenApiSchema {
    OpenApiSchema::object(
        BTreeMap::from([
            ("id".to_owned(), OpenApiSchema::integer()),
            ("title".to_owned(), OpenApiSchema::string()),
        ]),
        ["id".to_owned(), "title".to_owned()],
    )
    .unwrap()
}

#[test]
fn document_translates_routes_and_renders_json_contracts() {
    let document = OpenApiDocument::new("Todo API", "0.1.0")
        .unwrap()
        .component("Todo", todo_schema())
        .unwrap()
        .operation(
            OpenApiRoute::from_rustee("/todos/:todo_id").unwrap(),
            OpenApiMethod::Get,
            OpenApiOperation::builder("get_todo")
                .summary("Gets one todo")
                .tag("todos")
                .path_parameter("todo_id", OpenApiSchema::integer())
                .json_response(
                    StatusCode::OK,
                    "The requested todo",
                    OpenApiSchema::component_reference("Todo").unwrap(),
                )
                .empty_response(StatusCode::NOT_FOUND, "The todo was not found")
                .build()
                .unwrap(),
        )
        .unwrap();

    let document = document.to_value();
    assert_eq!(document["openapi"], "3.1.1");
    assert_eq!(
        document["paths"]["/todos/{todo_id}"]["get"]["operationId"],
        "get_todo"
    );
    assert_eq!(
        document["paths"]["/todos/{todo_id}"]["get"]["parameters"][0]["in"],
        "path"
    );
    assert_eq!(
        document["paths"]["/todos/{todo_id}"]["get"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/Todo"
    );
}

#[test]
fn document_rejects_missing_or_extraneous_path_parameters() {
    let operation = OpenApiOperation::builder("get_todo")
        .empty_response(StatusCode::NO_CONTENT, "No content")
        .build()
        .unwrap();
    assert_eq!(
        OpenApiDocument::new("Todo API", "0.1.0")
            .unwrap()
            .operation(
                OpenApiRoute::from_rustee("/todos/:todo_id").unwrap(),
                OpenApiMethod::Get,
                operation,
            )
            .unwrap_err(),
        OpenApiError::MissingPathParameter
    );

    let operation = OpenApiOperation::builder("list_todos")
        .path_parameter("todo_id", OpenApiSchema::integer())
        .empty_response(StatusCode::OK, "No content")
        .build()
        .unwrap();
    assert_eq!(
        OpenApiDocument::new("Todo API", "0.1.0")
            .unwrap()
            .operation(
                OpenApiRoute::from_rustee("/todos").unwrap(),
                OpenApiMethod::Get,
                operation,
            )
            .unwrap_err(),
        OpenApiError::ExtraneousPathParameter
    );
}

#[test]
fn document_rejects_duplicate_schema_components() {
    assert_eq!(
        OpenApiDocument::new("Todo API", "0.1.0")
            .unwrap()
            .component("Todo", todo_schema())
            .unwrap()
            .component("Todo", todo_schema())
            .unwrap_err(),
        OpenApiError::DuplicateComponent
    );
}

#[test]
fn document_rejects_operation_ids_reused_on_different_method_path_pairs() {
    let list = OpenApiOperation::builder("list_todos")
        .empty_response(StatusCode::OK, "Todos")
        .build()
        .unwrap();
    let search = OpenApiOperation::builder("list_todos")
        .empty_response(StatusCode::OK, "Search results")
        .build()
        .unwrap();

    assert_eq!(
        OpenApiDocument::new("Todo API", "0.1.0")
            .unwrap()
            .operation(
                OpenApiRoute::from_rustee("/todos").unwrap(),
                OpenApiMethod::Get,
                list,
            )
            .unwrap()
            .operation(
                OpenApiRoute::from_rustee("/todos/search").unwrap(),
                OpenApiMethod::Post,
                search,
            )
            .unwrap_err(),
        OpenApiError::DuplicateOperationId
    );
}

#[tokio::test]
async fn document_is_a_json_handler_response() {
    let document = OpenApiDocument::new("Todo API", "0.1.0").unwrap();
    let response = document.into_response();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[CONTENT_TYPE],
        "application/json; charset=utf-8"
    );
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["info"]["title"],
        "Todo API"
    );
}
