use proc_macro2::TokenStream;
use quote::quote;
use syn::{Attribute, Data, DeriveInput, Error, Fields, LitStr, Result, parse_quote};

use crate::crate_path::rustee_crate_path;

pub(crate) fn expand(input: DeriveInput) -> Result<TokenStream> {
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
