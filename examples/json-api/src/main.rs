use std::{
    net::SocketAddr,
    sync::atomic::{AtomicU64, Ordering},
};

use rustee::{App, Error, Json, Path, Query, Result, State, StatusCode};
use serde::{Deserialize, Serialize};

#[derive(Default)]
struct OrderSequence(AtomicU64);

#[derive(Deserialize)]
struct ProductPath {
    id: u64,
}

#[derive(Deserialize)]
struct ProductQuery {
    include_inventory: Option<bool>,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
struct Product {
    id: u64,
    name: String,
    inventory: Option<u32>,
}

#[derive(Debug, Deserialize, Serialize)]
struct CreateOrder {
    product_id: u64,
    quantity: u16,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
struct Order {
    id: u64,
    product_id: u64,
    quantity: u16,
}

async fn product(
    Path(path): Path<ProductPath>,
    Query(query): Query<ProductQuery>,
) -> Json<Product> {
    Json(Product {
        id: path.id,
        name: "Rustee field guide".to_owned(),
        inventory: query.include_inventory.unwrap_or(false).then_some(12),
    })
}

async fn create_order(
    State(sequence): State<OrderSequence>,
    Json(order): Json<CreateOrder>,
) -> Result<(StatusCode, Json<Order>)> {
    if order.quantity == 0 {
        return Err(Error::bad_request("quantity must be greater than zero"));
    }

    let id = sequence.0.fetch_add(1, Ordering::Relaxed) + 1;
    Ok((
        StatusCode::CREATED,
        Json(Order {
            id,
            product_id: order.product_id,
            quantity: order.quantity,
        }),
    ))
}

fn app() -> App {
    App::new()
        .with_state(OrderSequence::default())
        .get("/products/:id", product)
        .post("/orders", create_order)
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    rustee::serve(SocketAddr::from(([127, 0, 0, 1], 3001)), app()).await
}

#[cfg(test)]
mod tests {
    use rustee::StatusCode;
    use rustee_test::TestApp;

    use super::{CreateOrder, Order, Product, app};

    #[tokio::test]
    async fn typed_extractors_and_state_produce_the_documented_json_flow() {
        let client = TestApp::new(app());

        let product = client
            .get("/products/7?include_inventory=true")
            .unwrap()
            .send()
            .await
            .unwrap();
        product.assert_status(StatusCode::OK).unwrap();
        assert_eq!(
            product.json::<Product>().unwrap(),
            Product {
                id: 7,
                name: "Rustee field guide".to_owned(),
                inventory: Some(12),
            }
        );

        let created = client
            .post("/orders")
            .unwrap()
            .json(&CreateOrder {
                product_id: 7,
                quantity: 2,
            })
            .unwrap()
            .send()
            .await
            .unwrap();
        created.assert_status(StatusCode::CREATED).unwrap();
        assert_eq!(
            created.json::<Order>().unwrap(),
            Order {
                id: 1,
                product_id: 7,
                quantity: 2,
            }
        );

        let rejected = client
            .post("/orders")
            .unwrap()
            .json(&CreateOrder {
                product_id: 7,
                quantity: 0,
            })
            .unwrap()
            .send()
            .await
            .unwrap();
        rejected.assert_status(StatusCode::BAD_REQUEST).unwrap();
    }
}
