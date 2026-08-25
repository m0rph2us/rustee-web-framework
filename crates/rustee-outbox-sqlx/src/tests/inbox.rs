use crate::{InboxConsumer, InboxMessageId};

#[test]
fn inbox_keys_are_scoped_and_bounded() {
    let event_id = rustee_events::EventId::new();
    let job_id = rustee_jobs::JobId::new();
    assert_eq!(
        InboxMessageId::event(event_id).as_str(),
        event_id.to_string()
    );
    assert_eq!(InboxMessageId::job(job_id).as_str(), job_id.to_string());
    assert!(InboxConsumer::new(" ").is_err());
    assert!(InboxMessageId::new("\0").is_err());
}
