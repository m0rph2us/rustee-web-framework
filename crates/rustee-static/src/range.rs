//! Bounded HTTP byte-range parsing and representation metadata.

use http::{
    HeaderMap, HeaderValue,
    header::{IF_RANGE, RANGE},
};

use super::response::{CacheValidators, truncate_to_seconds};

const MAX_MULTIPART_RANGES: usize = 16;
pub(super) const MAX_RANGE_MEMBERS: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ByteRange {
    pub(super) start: u64,
    pub(super) end: u64,
}

impl ByteRange {
    pub(super) const fn len(self) -> u64 {
        self.end - self.start + 1
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum RequestedRange {
    Full,
    Partial(ByteRange),
    Multipart(Vec<ByteRange>),
    Unsatisfiable,
}

pub(super) fn requested_range(
    headers: &HeaderMap,
    length: u64,
    validators: Option<&CacheValidators>,
) -> RequestedRange {
    let values = headers.get_all(RANGE);
    let mut values = values.iter();
    let Some(value) = values.next() else {
        return RequestedRange::Full;
    };
    if values.next().is_some() {
        return RequestedRange::Unsatisfiable;
    }
    let Ok(value) = value.to_str() else {
        return RequestedRange::Unsatisfiable;
    };
    let value = value.trim();
    let Some((unit, specifier)) = value.split_once('=') else {
        return RequestedRange::Unsatisfiable;
    };
    if !unit.eq_ignore_ascii_case("bytes") {
        return RequestedRange::Full;
    }
    if !if_range_matches(headers, validators) {
        return RequestedRange::Full;
    }
    parse_range_set(specifier.trim(), length)
}

pub(super) fn content_range_header(range: ByteRange, length: u64) -> HeaderValue {
    HeaderValue::from_str(&format!("bytes {}-{}/{length}", range.start, range.end))
        .expect("formatted content range is a valid header")
}

pub(super) fn unsatisfied_content_range_header(length: u64) -> HeaderValue {
    HeaderValue::from_str(&format!("bytes */{length}"))
        .expect("formatted content range is a valid header")
}

fn parse_range_set(specifier: &str, length: u64) -> RequestedRange {
    let mut ranges = Vec::new();
    for (index, specifier) in specifier.split(',').enumerate() {
        if index >= MAX_RANGE_MEMBERS {
            return RequestedRange::Unsatisfiable;
        }
        let range = match parse_range_member(specifier.trim(), length) {
            Ok(Some(range)) => range,
            Ok(None) => continue,
            Err(()) => return RequestedRange::Unsatisfiable,
        };
        ranges.push(range);
    }
    if ranges.is_empty() {
        return RequestedRange::Unsatisfiable;
    }
    ranges.sort_unstable_by_key(|range| (range.start, range.end));
    let mut normalized: Vec<ByteRange> = Vec::with_capacity(ranges.len());
    for range in ranges {
        if let Some(previous) = normalized.last_mut()
            && range.start <= previous.end.saturating_add(1)
        {
            previous.end = previous.end.max(range.end);
        } else {
            normalized.push(range);
        }
    }
    if normalized.len() > MAX_MULTIPART_RANGES {
        return RequestedRange::Unsatisfiable;
    }
    match normalized.as_slice() {
        [range] => RequestedRange::Partial(*range),
        _ => RequestedRange::Multipart(normalized),
    }
}

fn parse_range_member(specifier: &str, length: u64) -> Result<Option<ByteRange>, ()> {
    let Some((start, end)) = specifier.split_once('-') else {
        return Err(());
    };
    if start.is_empty() {
        let suffix_length = end.parse::<u64>().map_err(|_| ())?;
        if suffix_length == 0 || length == 0 {
            return Ok(None);
        }
        let start = length.saturating_sub(suffix_length);
        return Ok(Some(ByteRange {
            start,
            end: length - 1,
        }));
    }

    let start = start.parse::<u64>().map_err(|_| ())?;
    if start >= length {
        return Ok(None);
    }
    let end = if end.is_empty() {
        length - 1
    } else {
        let end = end.parse::<u64>().map_err(|_| ())?;
        end.min(length - 1)
    };
    if end < start {
        return Err(());
    }
    Ok(Some(ByteRange { start, end }))
}

fn if_range_matches(headers: &HeaderMap, validators: Option<&CacheValidators>) -> bool {
    let values = headers.get_all(IF_RANGE);
    let mut values = values.iter();
    let Some(value) = values.next() else {
        return true;
    };
    if values.next().is_some() {
        return false;
    }
    let Some(validators) = validators else {
        return false;
    };
    let Ok(value) = value.to_str() else {
        return false;
    };
    let Ok(date) = httpdate::parse_http_date(value) else {
        return false;
    };
    truncate_to_seconds(validators.modified).is_some_and(|modified| modified <= date)
}
