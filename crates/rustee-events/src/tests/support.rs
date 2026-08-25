use serde::{Deserialize, Serialize};

use crate::Event;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct OrderPaid {
    pub(super) order_id: u64,
}

impl Event for OrderPaid {
    const TYPE: &'static str = "orders.paid";
    const VERSION: u16 = 1;
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct OrderPaidV2 {
    pub(super) order_id: u64,
    pub(super) currency: String,
}

impl Event for OrderPaidV2 {
    const TYPE: &'static str = "orders.paid";
    const VERSION: u16 = 2;
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct LargeEvent {
    pub(super) body: String,
}

impl Event for LargeEvent {
    const TYPE: &'static str = "large.event";
    const VERSION: u16 = 1;
}
