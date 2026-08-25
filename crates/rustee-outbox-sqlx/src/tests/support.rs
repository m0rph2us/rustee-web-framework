use rustee_events::Event;
use rustee_jobs::Job;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct OrderPaid;

impl Event for OrderPaid {
    const TYPE: &'static str = "orders.paid";
    const VERSION: u16 = 1;
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct PrivateOrderPaid {
    pub(super) account_note: String,
}

impl Event for PrivateOrderPaid {
    const TYPE: &'static str = "orders.private-paid";
    const VERSION: u16 = 1;
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct SendReceipt;

impl Job for SendReceipt {
    const NAME: &'static str = "receipts.send";
    const VERSION: u16 = 1;
}
