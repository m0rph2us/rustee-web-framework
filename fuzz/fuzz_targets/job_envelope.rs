#![no_main]

use std::convert::Infallible;

use libfuzzer_sys::fuzz_target;
use rustee_jobs::{Job, JobEnvelope};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Deserialize, Serialize)]
struct FuzzJob;

impl Job for FuzzJob {
    const NAME: &'static str = "fuzz.job";
    const VERSION: u16 = 1;
}

fuzz_target!(|data: &[u8]| {
    let _ = JobEnvelope::<FuzzJob>::decode(data);
    let upcaster = |_version: u16, _payload: Value| Ok::<FuzzJob, Infallible>(FuzzJob);
    let _ = JobEnvelope::<FuzzJob>::decode_compatible(data, &upcaster);
});
