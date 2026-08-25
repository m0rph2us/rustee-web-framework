use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    Error, Expr, Ident, Result, Token,
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
};

use crate::crate_path::rustee_crate_path;

pub(crate) struct RoutesInput {
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

pub(crate) fn expand(input: RoutesInput) -> Result<TokenStream> {
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
