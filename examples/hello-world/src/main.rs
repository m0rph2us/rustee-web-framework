use std::net::SocketAddr;

use rustee::{
    App, Json, StatusCode,
    openapi::{OpenApiDocument, OpenApiMethod, OpenApiOperation, OpenApiRoute, OpenApiSchema},
};
use serde::Serialize;

#[derive(Serialize)]
struct Greeting {
    message: &'static str,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let openapi = OpenApiDocument::new("Rustee hello world", "0.1.0")
        .expect("the static document metadata is valid")
        .operation(
            OpenApiRoute::from_rustee("/").expect("the root route is valid"),
            OpenApiMethod::Get,
            OpenApiOperation::builder("hello")
                .summary("Returns a greeting")
                .json_response(
                    StatusCode::OK,
                    "A JSON greeting",
                    OpenApiSchema::object(
                        std::collections::BTreeMap::from([(
                            "message".to_owned(),
                            OpenApiSchema::string(),
                        )]),
                        ["message".to_owned()],
                    )
                    .expect("the static greeting schema is valid"),
                )
                .build()
                .expect("the static operation is valid"),
        )
        .expect("the static OpenAPI document is valid");
    let app = App::new()
        .get("/", || async {
            Json(Greeting {
                message: "Hello from Rustee",
            })
        })
        .get("/openapi.json", move || {
            let openapi = openapi.clone();
            async move { openapi }
        });
    rustee::serve(SocketAddr::from(([127, 0, 0, 1], 3000)), app).await
}
