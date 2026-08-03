//! Optional builder macros and derives that implement existing Rustee contracts.
//!
//! The macros intentionally emit implementations against the `rustee` facade's hidden
//! re-exports. They do not infer handlers, middleware, authorization, or HTTP policy.

use proc_macro::TokenStream;

use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{
    Attribute, Data, DeriveInput, Error, Expr, Fields, Ident, LitStr, Result, Token,
    parse::{Parse, ParseStream},
    parse_macro_input, parse_quote,
    punctuated::Punctuated,
};

/// Chains explicit Rustee route registrations onto one application builder expression.
///
/// Every entry maps directly to the corresponding [`rustee::App`] method. The macro does not
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
    match expand_routes(parse_macro_input!(input as RoutesInput)) {
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
    match expand_from_header(parse_macro_input!(input as DeriveInput)) {
        Ok(output) => output.into(),
        Err(error) => error.into_compile_error().into(),
    }
}

fn expand_from_header(input: DeriveInput) -> Result<TokenStream2> {
    let header = required_header_attribute(&input.attrs)?;
    if http::header::HeaderName::from_bytes(header.value().as_bytes()).is_err() {
        return Err(Error::new_spanned(
            header,
            "`rustee(header)` must be a valid HTTP header name",
        ));
    }

    let field_ty = single_tuple_field(&input)?;
    let type_name = input.ident;
    let mut generics = input.generics;
    generics
        .make_where_clause()
        .predicates
        .push(parse_quote!(#field_ty: ::core::str::FromStr));
    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();
    let rustee = rustee_crate_path();
    let invalid_header_message =
        LitStr::new(&format!("invalid {} header", header.value()), header.span());

    Ok(quote! {
        impl #impl_generics #rustee::__private::FromHeader for #type_name #type_generics #where_clause {
            const NAME: &'static str = #header;

            fn from_header(
                value: &#rustee::__http::HeaderValue,
            ) -> #rustee::__private::Result<Self> {
                let value = value
                    .to_str()
                    .map_err(|_| #rustee::__private::Error::bad_request(#invalid_header_message))?;
                let parsed = <#field_ty as ::core::str::FromStr>::from_str(value)
                    .map_err(|_| #rustee::__private::Error::bad_request(#invalid_header_message))?;
                Ok(Self(parsed))
            }
        }
    })
}

fn required_header_attribute(attributes: &[Attribute]) -> Result<LitStr> {
    let mut header = None;

    for attribute in attributes
        .iter()
        .filter(|attribute| attribute.path().is_ident("rustee"))
    {
        attribute.parse_nested_meta(|meta| {
            if !meta.path.is_ident("header") {
                return Err(meta.error("expected `rustee(header = \"x-header\")`"));
            }
            if header.is_some() {
                return Err(meta.error("`rustee(header = ...)` may be declared only once"));
            }
            header = Some(meta.value()?.parse::<LitStr>()?);
            Ok(())
        })?;
    }

    header.ok_or_else(|| {
        Error::new(
            proc_macro2::Span::call_site(),
            "missing `#[rustee(header = \"x-header\")]` attribute",
        )
    })
}

fn single_tuple_field(input: &DeriveInput) -> Result<syn::Type> {
    match &input.data {
        Data::Struct(structure) => match &structure.fields {
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                Ok(fields.unnamed[0].ty.clone())
            }
            Fields::Unnamed(fields) => Err(Error::new_spanned(
                fields,
                "`FromHeader` requires a tuple newtype with exactly one field",
            )),
            Fields::Named(fields) => Err(Error::new_spanned(
                fields,
                "`FromHeader` requires a tuple newtype with exactly one field",
            )),
            Fields::Unit => Err(Error::new_spanned(
                &input.ident,
                "`FromHeader` requires a tuple newtype with exactly one field",
            )),
        },
        Data::Enum(_) | Data::Union(_) => Err(Error::new_spanned(
            input,
            "`FromHeader` can only be derived for a tuple newtype",
        )),
    }
}

fn rustee_crate_path() -> TokenStream2 {
    match crate_name("rustee") {
        Ok(FoundCrate::Itself) => quote!(crate),
        Ok(FoundCrate::Name(name)) => {
            let ident = format_ident!("{}", name.replace('-', "_"));
            quote!(::#ident)
        }
        Err(_) => quote!(::rustee),
    }
}

struct RoutesInput {
    app: Expr,
    routes: Vec<RouteSpec>,
}

impl Parse for RoutesInput {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let app = input.parse()?;
        input.parse::<Token![;]>()?;
        let routes = Punctuated::<RouteSpec, Token![,]>::parse_terminated(input)?
            .into_iter()
            .collect::<Vec<_>>();
        if routes.is_empty() {
            return Err(Error::new_spanned(
                app,
                "`routes!` requires at least one route registration",
            ));
        }
        Ok(Self { app, routes })
    }
}

struct RouteSpec {
    method: Ident,
    path: Expr,
    handler: Expr,
}

impl Parse for RouteSpec {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        Ok(Self {
            method: input.parse()?,
            path: input.parse()?,
            handler: {
                input.parse::<Token![=>]>()?;
                input.parse()?
            },
        })
    }
}

fn expand_routes(input: RoutesInput) -> Result<TokenStream2> {
    let RoutesInput {
        app: app_expr,
        routes,
    } = input;
    let rustee = rustee_crate_path();
    let mut app = quote!(#app_expr);

    for route in routes {
        let method = route.method.to_string();
        let path = route.path;
        let handler = route.handler;
        let registration = match method.as_str() {
            "GET" => quote!(.get(#path, #handler)),
            "POST" => quote!(.post(#path, #handler)),
            "PUT" => quote!(.put(#path, #handler)),
            "PATCH" => quote!(.patch(#path, #handler)),
            "DELETE" => quote!(.delete(#path, #handler)),
            "HEAD" => quote!(.route(#rustee::Method::HEAD, #path, #handler)),
            "OPTIONS" => quote!(.route(#rustee::Method::OPTIONS, #path, #handler)),
            "TRACE" => quote!(.route(#rustee::Method::TRACE, #path, #handler)),
            "CONNECT" => quote!(.route(#rustee::Method::CONNECT, #path, #handler)),
            _ => {
                return Err(Error::new_spanned(
                    route.method,
                    "`routes!` supports GET, HEAD, POST, PUT, PATCH, DELETE, OPTIONS, TRACE, and CONNECT; use `App::route` for a custom method",
                ));
            }
        };
        app = quote!(#app #registration);
    }

    Ok(quote!({ #app }))
}
