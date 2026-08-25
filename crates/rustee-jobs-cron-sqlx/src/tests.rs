use std::{
    num::{NonZeroU32, NonZeroUsize},
    time::Duration,
};

use chrono::DateTime;
use rustee_outbox_sqlx::OutboxDestination;
use serde::{Deserialize, Serialize};

use super::{
    CronExpression, CronExpressionError, RecurringJob, RecurringJobError, RecurringJobFireLimit,
    RecurringJobFireLimitError, RecurringJobId, RecurringJobKey, RecurringJobKeyError,
    RecurringJobRateLimit, RecurringJobRateLimitError, RecurringJobRateLimitKey,
    RecurringJobRegistration, RecurringJobTimeZone, RecurringJobTimeZoneError, fixed_window_bounds,
};

#[derive(Deserialize, Serialize)]
struct RedactedReminder {
    secret: String,
}

impl rustee_jobs::Job for RedactedReminder {
    const NAME: &'static str = "billing.redacted-reminder";
    const VERSION: u16 = 1;
}

#[test]
fn cron_expression_is_bounded_and_has_a_future_utc_occurrence() {
    assert_eq!(
        CronExpression::new("not cron").unwrap_err(),
        CronExpressionError::InvalidExpression
    );
    let expression = CronExpression::new("0 * * * * * *").unwrap();
    let next = expression
        .next_after_unix_ms(1_722_643_200_000, &RecurringJobTimeZone::default())
        .unwrap();
    assert!(next > 1_722_643_200_000);
}

#[test]
fn iana_time_zone_skips_dst_gaps_and_fires_an_ambiguous_wall_time_once() {
    let new_york = RecurringJobTimeZone::new("America/New_York").unwrap();
    let at_two_thirty = CronExpression::new("0 30 2 * * * *").unwrap();
    let before_spring_gap = unix_ms("2026-03-08T06:00:00Z");
    assert_eq!(
        at_two_thirty
            .next_after_unix_ms(before_spring_gap, &new_york)
            .unwrap(),
        unix_ms("2026-03-09T06:30:00Z")
    );

    let at_one_thirty = CronExpression::new("0 30 1 * * * *").unwrap();
    let before_fall_overlap = unix_ms("2026-11-01T04:00:00Z");
    let earlier_occurrence = unix_ms("2026-11-01T05:30:00Z");
    assert_eq!(
        at_one_thirty
            .next_after_unix_ms(before_fall_overlap, &new_york)
            .unwrap(),
        earlier_occurrence
    );
    assert_eq!(
        at_one_thirty
            .next_after_unix_ms(earlier_occurrence, &new_york)
            .unwrap(),
        unix_ms("2026-11-02T06:30:00Z")
    );
}

#[test]
fn recurring_time_zones_are_iana_names_and_default_to_utc() {
    assert_eq!(RecurringJobTimeZone::default().as_str(), "UTC");
    assert_eq!(
        RecurringJobTimeZone::new("not/a-time-zone").unwrap_err(),
        RecurringJobTimeZoneError::InvalidTimeZone
    );
}

#[test]
fn schedule_keys_and_fire_limits_are_bounded() {
    assert_eq!(
        RecurringJobKey::new(" ").unwrap_err(),
        RecurringJobKeyError::InvalidKey
    );
    assert_eq!(
        RecurringJobFireLimit::new(NonZeroUsize::new(101).unwrap()).unwrap_err(),
        RecurringJobFireLimitError::TooLarge
    );
    assert_eq!(RecurringJobFireLimit::default().get().get(), 25);
}

#[test]
fn schedule_key_diagnostics_redact_the_application_owned_key() {
    let key = RecurringJobKey::new("billing.account-123.monthly-reminder").unwrap();
    let debug = format!("{key:?}");

    assert!(!debug.contains("billing.account-123.monthly-reminder"));
    assert!(debug.contains("byte_len"));
}

#[test]
fn durable_schedule_id_diagnostics_redact_the_identifier_through_registration_outcomes() {
    let id = RecurringJobId::new();
    let raw_id = id.to_string();

    for output in [
        format!("{id:?}"),
        format!("{:?}", RecurringJobRegistration::Registered(id)),
        format!("{:?}", RecurringJobRegistration::AlreadyPresent(id)),
    ] {
        assert!(!output.contains(&raw_id));
        assert!(output.contains("RecurringJobId([REDACTED])"));
    }
}

#[test]
fn rate_governor_policy_and_fixed_window_boundaries_are_bounded() {
    assert!(RecurringJobRateLimitKey::new(" ").is_err());
    let key = RecurringJobRateLimitKey::new("provider.billing").unwrap();
    assert_eq!(
        RecurringJobRateLimit::new(key, NonZeroU32::new(1).unwrap(), Duration::ZERO).unwrap_err(),
        RecurringJobRateLimitError::InvalidWindow
    );
    assert_eq!(
        RecurringJobRateLimit::new(
            RecurringJobRateLimitKey::new("provider.billing.sub-millisecond").unwrap(),
            NonZeroU32::new(1).unwrap(),
            Duration::from_nanos(1),
        )
        .unwrap_err(),
        RecurringJobRateLimitError::InvalidWindow
    );
    assert_eq!(
        RecurringJobRateLimit::new(
            RecurringJobRateLimitKey::new("provider.billing.fractional-millisecond").unwrap(),
            NonZeroU32::new(1).unwrap(),
            Duration::from_millis(1) + Duration::from_nanos(1),
        )
        .unwrap_err(),
        RecurringJobRateLimitError::InvalidWindow
    );
    let minimum_window = RecurringJobRateLimit::new(
        RecurringJobRateLimitKey::new("provider.billing.minimum-window").unwrap(),
        NonZeroU32::new(1).unwrap(),
        Duration::from_millis(1),
    )
    .unwrap();
    assert_eq!(minimum_window.window(), Duration::from_millis(1));
    assert_eq!(
        RecurringJobRateLimit::new(
            RecurringJobRateLimitKey::new("provider.billing.large").unwrap(),
            NonZeroU32::new(i32::MAX as u32 + 1).unwrap(),
            Duration::from_secs(60),
        )
        .unwrap_err(),
        RecurringJobRateLimitError::CapacityTooLarge
    );
    assert_eq!(
        fixed_window_bounds(125_001, 60_000).unwrap(),
        (120_000, 180_000)
    );
    assert_eq!(fixed_window_bounds(-1, 60_000).unwrap(), (-60_000, 0));
    assert!(matches!(
        fixed_window_bounds(0, 0),
        Err(RecurringJobError::StoredSchedule)
    ));
}

#[test]
fn rate_governor_diagnostics_redact_the_application_owned_key() {
    let key = RecurringJobRateLimitKey::new("provider.billing.account-123").unwrap();
    let policy = RecurringJobRateLimit::new(
        key.clone(),
        NonZeroU32::new(5).unwrap(),
        Duration::from_secs(60),
    )
    .unwrap();

    for output in [format!("{key:?}"), format!("{policy:?}")] {
        assert!(!output.contains("provider.billing.account-123"));
        assert!(output.contains("byte_len"));
    }
}

#[test]
fn recurring_job_debug_never_renders_the_template_payload() {
    let job = RecurringJob::new(
        RecurringJobKey::new("billing.redacted-reminder").unwrap(),
        OutboxDestination::new("jobs.billing").unwrap(),
        RedactedReminder {
            secret: "not-for-debug".to_owned(),
        },
        CronExpression::new("* * * * * * *").unwrap(),
    );

    let debug = format!("{job:?}");
    assert!(!debug.contains("billing.redacted-reminder"));
    assert!(!debug.contains("not-for-debug"));
}

fn unix_ms(value: &str) -> i64 {
    DateTime::parse_from_rfc3339(value)
        .unwrap()
        .timestamp_millis()
}
