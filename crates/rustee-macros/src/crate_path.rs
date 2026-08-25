use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

pub(crate) fn rustee_crate_path() -> TokenStream {
    match crate_name("rustee") {
        Ok(FoundCrate::Itself) => quote!(crate),
        Ok(FoundCrate::Name(name)) => {
            let ident = format_ident!("{}", name.replace('-', "_"));
            quote!(::#ident)
        }
        Err(_) => quote!(::rustee),
    }
}
