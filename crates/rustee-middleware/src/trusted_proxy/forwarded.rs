//! Bounded RFC 7239 and X-Forwarded header decoding for trusted proxy peers.

use std::{net::IpAddr, str::FromStr};

use http::{HeaderMap, HeaderValue, header::FORWARDED};

use super::{ForwardedContext, TrustedProxyPolicy};

const MAX_FORWARDED_HEADER_BYTES: usize = 2_048;
pub(crate) const X_FORWARDED_FOR: &str = "x-forwarded-for";
pub(crate) const X_FORWARDED_HOST: &str = "x-forwarded-host";
pub(crate) const X_FORWARDED_PROTO: &str = "x-forwarded-proto";

#[derive(Debug)]
struct ForwardedElement {
    client_ip: IpAddr,
    scheme: Option<String>,
    host: Option<String>,
}

pub(super) fn parse_forwarded_headers(
    headers: &HeaderMap,
    policy: &TrustedProxyPolicy,
) -> Result<Option<ForwardedContext>, ()> {
    let value = single_header(headers, FORWARDED)?;
    value
        .map(|value| parse_forwarded(value, policy))
        .transpose()
}

fn parse_forwarded(
    value: &HeaderValue,
    policy: &TrustedProxyPolicy,
) -> Result<ForwardedContext, ()> {
    let value = bounded_header_text(value)?;
    let elements = value
        .split(',')
        .map(|element| parse_forwarded_element(element.trim()))
        .collect::<Result<Vec<_>, _>>()?;
    if elements.is_empty() || elements.len() > policy.forwarded_chain_hops() + 1 {
        return Err(());
    }
    if elements[..elements.len() - 1]
        .iter()
        .any(|element| element.scheme.is_some() || element.host.is_some())
    {
        return Err(());
    }
    let edge = elements.last().ok_or(())?;
    let mut trusted_hops = 0;
    for element in elements.iter().rev() {
        if trusted_hops < policy.forwarded_chain_hops() && policy.trusts(element.client_ip) {
            trusted_hops += 1;
            continue;
        }
        return Ok(ForwardedContext::new(
            element.client_ip,
            edge.scheme.clone(),
            edge.host.clone(),
        ));
    }
    Err(())
}

pub(super) fn parse_x_forwarded_headers(
    headers: &HeaderMap,
    policy: &TrustedProxyPolicy,
) -> Result<Option<ForwardedContext>, ()> {
    let client = single_header(headers, X_FORWARDED_FOR)?;
    let scheme = single_header(headers, X_FORWARDED_PROTO)?;
    let host = single_header(headers, X_FORWARDED_HOST)?;
    let Some(client) = client else {
        return (scheme.is_none() && host.is_none())
            .then_some(None)
            .ok_or(());
    };
    let client = bounded_header_text(client)?;
    let clients = client
        .split(',')
        .map(str::trim)
        .map(|value| IpAddr::from_str(value).map_err(|_| ()))
        .collect::<Result<Vec<_>, _>>()?;
    if clients.is_empty() || clients.len() > policy.forwarded_chain_hops() + 1 {
        return Err(());
    }
    let client_ip = select_client_ip(&clients, policy)?;
    let scheme = scheme.map(parse_x_forwarded_scheme).transpose()?;
    let host = host.map(parse_x_forwarded_host).transpose()?;
    Ok(Some(ForwardedContext::new(client_ip, scheme, host)))
}

fn single_header(
    headers: &HeaderMap,
    name: impl http::header::AsHeaderName,
) -> Result<Option<&HeaderValue>, ()> {
    let values = headers.get_all(name);
    let mut values = values.iter();
    let first = values.next();
    values.next().is_none().then_some(first).ok_or(())
}

fn select_client_ip(clients: &[IpAddr], policy: &TrustedProxyPolicy) -> Result<IpAddr, ()> {
    let mut trusted_hops = 0;
    for client in clients.iter().rev() {
        if trusted_hops < policy.forwarded_chain_hops() && policy.trusts(*client) {
            trusted_hops += 1;
            continue;
        }
        return Ok(*client);
    }
    Err(())
}

fn parse_x_forwarded_scheme(value: &HeaderValue) -> Result<String, ()> {
    let value = bounded_header_text(value)?;
    matches!(value, "http" | "https")
        .then(|| value.to_owned())
        .ok_or(())
}

fn parse_x_forwarded_host(value: &HeaderValue) -> Result<String, ()> {
    let value = bounded_header_text(value)?;
    (!value.contains([',', ' ', '"', '@']) && value.parse::<http::uri::Authority>().is_ok())
        .then(|| value.to_owned())
        .ok_or(())
}

fn bounded_header_text(value: &HeaderValue) -> Result<&str, ()> {
    let value = value.to_str().map_err(|_| ())?;
    (value.len() <= MAX_FORWARDED_HEADER_BYTES)
        .then_some(value)
        .ok_or(())
}

fn parse_forwarded_element(value: &str) -> Result<ForwardedElement, ()> {
    let mut client_ip = None;
    let mut scheme = None;
    let mut host = None;
    for item in value.split(';') {
        let (name, value) = item.trim().split_once('=').ok_or(())?;
        if value.is_empty() || value.contains([' ', '"']) {
            return Err(());
        }
        match name.to_ascii_lowercase().as_str() {
            "for" if client_ip.is_none() => {
                client_ip = Some(IpAddr::from_str(value).map_err(|_| ())?);
            }
            "proto" if scheme.is_none() && matches!(value, "http" | "https") => {
                scheme = Some(value.to_owned());
            }
            "host"
                if host.is_none()
                    && !value.contains('@')
                    && value.parse::<http::uri::Authority>().is_ok() =>
            {
                host = Some(value.to_owned());
            }
            _ => return Err(()),
        }
    }
    Ok(ForwardedElement {
        client_ip: client_ip.ok_or(())?,
        scheme,
        host,
    })
}
