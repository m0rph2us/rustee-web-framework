//! Compile-checked hello-world Rustee service with an `OpenAPI` description.

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
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let openapi = OpenApiDocument::new("Rustee hello world", "0.1.0")?.operation(
        OpenApiRoute::from_rustee("/")?,
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
                )?,
            )
            .build()?,
    )?;
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
    rustee::serve(SocketAddr::from(([127, 0, 0, 1], 3000)), app).await?;
    Ok(())
}
