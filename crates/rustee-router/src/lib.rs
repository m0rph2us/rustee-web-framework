//! Route matching and typed handler dispatch.

mod app;
mod dispatch;
mod handler;
mod nesting;
mod pattern;

pub use app::{App, RouteError};
pub use handler::Handler;

#[cfg(test)]
use pattern::RoutePattern;

#[cfg(test)]
mod tests;
