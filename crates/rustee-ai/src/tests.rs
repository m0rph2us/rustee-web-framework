//! Unit-test entry point; shared fixtures live separately from feature assertions.

#[path = "tests/support.rs"]
mod support;

use support::*;

#[path = "tests/pipeline.rs"]
mod pipeline;
#[path = "tests/tools.rs"]
mod tools;
#[path = "tests/usage.rs"]
mod usage;
