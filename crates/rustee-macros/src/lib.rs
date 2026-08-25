//! Optional builder macros and derives that implement existing Rustee contracts.
//!
//! The macros intentionally emit implementations against the `rustee` facade's hidden
//! re-exports. They do not infer handlers, middleware, authorization, or HTTP policy.

mod crate_path;
mod from_header;
mod route_builder;

use proc_macro::TokenStream;
use syn::{DeriveInput, parse_macro_input};

/// Chains explicit Rustee route registrations onto one application builder expression.
///
/// Every entry maps directly to the corresponding `rustee::App` method. The macro does not
/// inspect the handler signature or validate route patterns beyond the runtime behavior already
/// provided by `App`.
///
/// ```ignore
/// let app = rustee::routes!(
///     rustee::App::new();
///     GET "/todos" => list_todos,
///     POST "/todos" => create_todo,
/// );
/// ```
#[proc_macro]
pub fn routes(input: TokenStream) -> TokenStream {
    match route_builder::expand(parse_macro_input!(input as route_builder::RoutesInput)) {
        Ok(output) => output.into(),
        Err(error) => error.into_compile_error().into(),
    }
}

/// Implements Rustee's `FromHeader` trait for a single-field tuple newtype.
///
/// The newtype field must implement [`FromStr`](core::str::FromStr), and the header name must be
/// declared explicitly:
///
/// ```ignore
/// #[derive(rustee::FromHeader)]
/// #[rustee(header = "x-request-id")]
/// struct RequestId(u64);
/// ```
#[proc_macro_derive(FromHeader, attributes(rustee))]
pub fn derive_from_header(input: TokenStream) -> TokenStream {
    match from_header::expand(parse_macro_input!(input as DeriveInput)) {
        Ok(output) => output.into(),
        Err(error) => error.into_compile_error().into(),
    }
}
