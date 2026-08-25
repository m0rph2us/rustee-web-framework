//! Private route-pattern parsing, matching, and nested-prefix validation.

use std::collections::BTreeSet;

use rustee_core::RouteParams;

use super::RouteError;

#[derive(Clone, Debug)]
pub(super) enum Segment {
    Static(String),
    Parameter(String),
}

#[derive(Clone, Debug)]
pub(super) struct RoutePattern {
    pub(super) segments: Vec<Segment>,
    pub(super) static_segments: usize,
}

impl RoutePattern {
    pub(super) fn parse(path: &str) -> std::result::Result<Self, RouteError> {
        if !path.starts_with('/') {
            return Err(RouteError::new("route paths must start with '/'"));
        }
        if path.contains('?') || path.contains('#') {
            return Err(RouteError::new(
                "route paths cannot contain a query string or fragment",
            ));
        }
        if path.contains("//") {
            return Err(RouteError::new(
                "route paths cannot contain repeated '/' separators",
            ));
        }

        let mut static_segments = 0;
        let mut names = BTreeSet::new();
        let mut segments = Vec::new();
        for segment in path
            .trim_matches('/')
            .split('/')
            .filter(|segment| !segment.is_empty())
        {
            if let Some(name) = segment.strip_prefix(':') {
                if name.is_empty()
                    || !name
                        .chars()
                        .all(|character| character.is_ascii_alphanumeric() || character == '_')
                {
                    return Err(RouteError::new(
                        "route parameter names must contain only ASCII letters, digits, or underscores",
                    ));
                }
                if !names.insert(name.to_owned()) {
                    return Err(RouteError::new("route parameter names must be unique"));
                }
                segments.push(Segment::Parameter(name.to_owned()));
            } else {
                static_segments += 1;
                segments.push(Segment::Static(segment.to_owned()));
            }
        }

        Ok(Self {
            segments,
            static_segments,
        })
    }

    pub(super) fn matches(&self, path: &str) -> Option<RouteParams> {
        if path.contains("//") {
            return None;
        }
        let mut incoming = path
            .trim_matches('/')
            .split('/')
            .filter(|segment| !segment.is_empty());
        let mut params = Vec::new();
        for segment in &self.segments {
            let incoming = incoming.next()?;
            match segment {
                Segment::Static(expected) if expected == incoming => {}
                Segment::Static(_) => return None,
                Segment::Parameter(name) => params.push((name.clone(), incoming.to_owned())),
            }
        }
        incoming.next().is_none().then(|| RouteParams::new(params))
    }

    /// Returns whether two patterns match exactly the same set of request paths.
    ///
    /// Parameter names are extraction labels, not part of route selection, so `/:id` and
    /// `/:user_id` are equivalent patterns.
    pub(super) fn is_equivalent_to(&self, other: &Self) -> bool {
        self.segments.len() == other.segments.len()
            && self
                .segments
                .iter()
                .zip(&other.segments)
                .all(|(left, right)| match (left, right) {
                    (Segment::Static(left), Segment::Static(right)) => left == right,
                    (Segment::Parameter(_), Segment::Parameter(_)) => true,
                    _ => false,
                })
    }
}

#[derive(Clone, Debug)]
pub(super) struct NestedPrefix {
    pub(super) value: String,
    pub(super) segment_count: usize,
}

impl NestedPrefix {
    pub(super) fn parse(path: &str) -> std::result::Result<Self, RouteError> {
        let pattern = RoutePattern::parse(path)?;
        if pattern.segments.is_empty() {
            return Err(RouteError::new(
                "nest prefixes must contain at least one static path segment",
            ));
        }
        if !pattern
            .segments
            .iter()
            .all(|segment| matches!(segment, Segment::Static(_)))
        {
            return Err(RouteError::new(
                "nest prefixes cannot contain route parameters",
            ));
        }
        let value = format!(
            "/{}",
            pattern
                .segments
                .iter()
                .map(|segment| match segment {
                    Segment::Static(value) => value.as_str(),
                    Segment::Parameter(_) => unreachable!("parameter prefixes were rejected"),
                })
                .collect::<Vec<_>>()
                .join("/")
        );
        Ok(Self {
            value,
            segment_count: pattern.segments.len(),
        })
    }

    pub(super) fn strip<'a>(&self, path: &'a str) -> Option<&'a str> {
        let remaining = path.strip_prefix(&self.value)?;
        if remaining.is_empty() {
            Some("/")
        } else if remaining.starts_with('/') {
            Some(remaining)
        } else {
            None
        }
    }
}
