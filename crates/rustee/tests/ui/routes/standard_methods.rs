use rustee::StatusCode;

async fn no_content() -> StatusCode {
    StatusCode::NO_CONTENT
}

fn main() {
    let _app = rustee::routes!(
        rustee::App::new();
        GET "/tasks" => no_content,
        HEAD "/tasks" => no_content,
        POST "/tasks" => no_content,
        PUT "/tasks/:id" => no_content,
        PATCH "/tasks/:id" => no_content,
        DELETE "/tasks/:id" => no_content,
        OPTIONS "/tasks" => no_content,
        TRACE "/tasks" => no_content,
        CONNECT "/tasks" => no_content,
    );
}
