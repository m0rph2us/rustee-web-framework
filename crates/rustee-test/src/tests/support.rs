use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub(super) struct Greeting {
    pub(super) name: String,
}
