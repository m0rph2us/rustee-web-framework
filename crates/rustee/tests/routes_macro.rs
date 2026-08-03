#![cfg(feature = "macros")]

use rustee::{App, Method, StatusCode};

async fn no_content() -> StatusCode {
    StatusCode::NO_CONTENT
}

async fn accepted() -> StatusCode {
    StatusCode::ACCEPTED
}

#[tokio::test]
async fn routes_macro_preserves_app_registration_and_dynamic_path_behavior() {
    let path = "/tasks";
    let app = rustee::routes!(
        App::new();
        GET path => no_content,
        OPTIONS "/tasks" => accepted,
    );

    let get = app
        .call(
            rustee::__http::Request::builder()
                .method(Method::GET)
                .uri("/tasks")
                .body(rustee::empty_body())
                .expect("a fixed request is valid"),
        )
        .await;
    assert_eq!(get.status(), StatusCode::NO_CONTENT);

    let options = app
        .call(
            rustee::__http::Request::builder()
                .method(Method::OPTIONS)
                .uri("/tasks")
                .body(rustee::empty_body())
                .expect("a fixed request is valid"),
        )
        .await;
    assert_eq!(options.status(), StatusCode::ACCEPTED);
}
