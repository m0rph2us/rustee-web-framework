use app::FromHeader;

#[derive(app::FromHeader, Debug, Eq, PartialEq)]
#[rustee(header = "x-request-id")]
struct RequestId(u64);

async fn no_content() -> app::StatusCode {
    app::StatusCode::NO_CONTENT
}

fn aliased_app() -> app::App {
    app::routes!(app::App::new(); GET "/alias" => no_content)
}

fn main() {
    let _ = <RequestId as FromHeader>::NAME;
    let _ = aliased_app();
}

#[test]
fn generated_code_uses_the_renamed_facade_dependency() {
    let parsed =
        <RequestId as FromHeader>::from_header(&app::__http::HeaderValue::from_static("9"));
    assert_eq!(parsed, Ok(RequestId(9)));
}
