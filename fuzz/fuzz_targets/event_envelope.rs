#![no_main]

use std::convert::Infallible;

use libfuzzer_sys::fuzz_target;
use rustee_events::{Event, EventEnvelope};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Deserialize, Serialize)]
struct FuzzEvent;

impl Event for FuzzEvent {
    const TYPE: &'static str = "fuzz.event";
    const VERSION: u16 = 1;
}

fuzz_target!(|data: &[u8]| {
    let _ = EventEnvelope::<FuzzEvent>::decode(data);
    let upcaster = |_version: u16, _payload: Value| Ok::<FuzzEvent, Infallible>(FuzzEvent);
    let _ = EventEnvelope::<FuzzEvent>::decode_compatible(data, &upcaster);
});
