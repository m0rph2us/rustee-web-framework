use std::{num::NonZeroUsize, time::Duration};

use crate::{
    EventSchedule, EventScheduleError, JobSchedule, JobScheduleError, LeaseConfig, RelayConfig,
    RelayConfigError, RelayLoopConfig, RelayLoopConfigError,
};

#[test]
fn relay_limits_are_bounded() {
    assert!(LeaseConfig::new(NonZeroUsize::new(1_001).unwrap(), Duration::from_secs(1),).is_err());
    let immediate = RelayConfig::new(LeaseConfig::default(), Duration::ZERO).unwrap();
    assert_eq!(immediate.retry_delay(), Duration::ZERO);
    let minimum_delayed =
        RelayConfig::new(LeaseConfig::default(), Duration::from_millis(1)).unwrap();
    assert_eq!(minimum_delayed.retry_delay(), Duration::from_millis(1));
    assert_eq!(
        RelayConfig::new(LeaseConfig::default(), Duration::from_nanos(1)),
        Err(RelayConfigError::InvalidRetryDelay)
    );
    assert!(RelayConfig::new(LeaseConfig::default(), Duration::from_secs(3_601)).is_err());
}

#[test]
fn delayed_job_schedules_are_positive_and_bounded() {
    assert_eq!(
        JobSchedule::after(Duration::ZERO),
        Err(JobScheduleError::ZeroDelay)
    );
    assert_eq!(
        JobSchedule::after(Duration::from_hours(8_808)),
        Err(JobScheduleError::DelayTooLong)
    );
    let schedule = JobSchedule::after(Duration::from_mins(1)).unwrap();
    assert_eq!(schedule.delay(), Duration::from_mins(1));
}

#[test]
fn delayed_event_schedules_are_positive_and_bounded() {
    assert_eq!(
        EventSchedule::after(Duration::ZERO),
        Err(EventScheduleError::ZeroDelay)
    );
    assert_eq!(
        EventSchedule::after(Duration::from_hours(8_808)),
        Err(EventScheduleError::DelayTooLong)
    );
    let schedule = EventSchedule::after(Duration::from_mins(1)).unwrap();
    assert_eq!(schedule.delay(), Duration::from_mins(1));
}

#[test]
fn relay_loop_config_requires_a_bounded_non_zero_idle_delay() {
    assert_eq!(
        RelayLoopConfig::new(Duration::ZERO),
        Err(RelayLoopConfigError::ZeroIdleDelay)
    );
    assert_eq!(
        RelayLoopConfig::new(Duration::from_secs(60 * 60 + 1)),
        Err(RelayLoopConfigError::IdleDelayTooLong)
    );
    let config = RelayLoopConfig::new(Duration::from_millis(25)).unwrap();
    assert_eq!(config.idle_delay(), Duration::from_millis(25));
}
