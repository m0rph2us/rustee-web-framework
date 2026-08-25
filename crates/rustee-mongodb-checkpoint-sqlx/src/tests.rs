use std::{error::Error as StdError, time::Duration};

use rustee_mongodb::mongodb::{bson, change_stream::event::ResumeToken};
use sqlx::Error as SqlxError;

use crate::{
    CHANGE_STREAM_CHECKPOINT_RESUME_TOKEN_BOUND_MIGRATION_SQL, ChangeStreamLeaseDuration,
    ChangeStreamLeaseOwner, ChangeStreamLeaseOwnerError, MAX_CHANGE_STREAM_RESUME_TOKEN_BYTES,
    PostgresChangeStreamCheckpointError,
    store::{decode_resume_token, encode_resume_token},
};

#[test]
fn opaque_resume_token_round_trips_as_bson_bytes() {
    let token = bson::from_document::<ResumeToken>(bson::doc! { "_data": "checkpoint-7" }).unwrap();
    let bytes = encode_resume_token(&token).unwrap();
    let restored = decode_resume_token(&bytes).unwrap();
    assert_eq!(restored, token);
    assert_eq!(
        decode_resume_token(&[0_u8, 1, 2]).unwrap_err().to_string(),
        "stored MongoDB change-stream checkpoint is invalid"
    );
}

#[test]
fn checkpoint_token_bytes_are_bounded_before_persistence_or_decoding() {
    let oversized = bson::from_document::<ResumeToken>(bson::doc! {
        "_data": "x".repeat(MAX_CHANGE_STREAM_RESUME_TOKEN_BYTES),
    })
    .unwrap();

    assert!(matches!(
        encode_resume_token(&oversized),
        Err(PostgresChangeStreamCheckpointError::InvalidCheckpoint)
    ));
    assert!(matches!(
        decode_resume_token(&vec![0_u8; MAX_CHANGE_STREAM_RESUME_TOKEN_BYTES + 1]),
        Err(PostgresChangeStreamCheckpointError::InvalidCheckpoint)
    ));
}

#[test]
fn checkpoint_migration_enforces_the_same_token_byte_bound_as_storage() {
    assert!(
        CHANGE_STREAM_CHECKPOINT_RESUME_TOKEN_BOUND_MIGRATION_SQL
            .contains(&MAX_CHANGE_STREAM_RESUME_TOKEN_BYTES.to_string(),)
    );
}

#[test]
fn checkpoint_diagnostics_redact_storage_details_and_preserve_sources() {
    let error = PostgresChangeStreamCheckpointError::Storage(SqlxError::Protocol(
        "private-checkpoint-storage-detail".to_owned(),
    ));

    assert!(!format!("{error:?}").contains("private-checkpoint-storage-detail"));
    assert!(
        !error
            .to_string()
            .contains("private-checkpoint-storage-detail")
    );
    assert!(StdError::source(&error).is_some());
}

#[test]
fn lease_metadata_is_bounded_and_redacted() {
    assert_eq!(
        ChangeStreamLeaseOwner::new(" ").unwrap_err(),
        ChangeStreamLeaseOwnerError::InvalidOwner
    );
    assert_eq!(
        ChangeStreamLeaseOwner::new("worker\0a").unwrap_err(),
        ChangeStreamLeaseOwnerError::InvalidOwner
    );
    assert_eq!(
        ChangeStreamLeaseOwner::new("a".repeat(256)).unwrap_err(),
        ChangeStreamLeaseOwnerError::InvalidOwner
    );
    let owner = ChangeStreamLeaseOwner::new("pod-7-attempt-3").unwrap();
    assert!(!format!("{owner:?}").contains("pod-7-attempt-3"));
    assert!(matches!(
        ChangeStreamLeaseDuration::new(Duration::ZERO).unwrap_err(),
        PostgresChangeStreamCheckpointError::InvalidLeaseDuration
    ));
    assert!(matches!(
        ChangeStreamLeaseDuration::new(Duration::from_nanos(1)).unwrap_err(),
        PostgresChangeStreamCheckpointError::InvalidLeaseDuration
    ));
    assert!(matches!(
        ChangeStreamLeaseDuration::new(Duration::from_millis(1) + Duration::from_nanos(1))
            .unwrap_err(),
        PostgresChangeStreamCheckpointError::InvalidLeaseDuration
    ));
    assert!(matches!(
        ChangeStreamLeaseDuration::new(Duration::from_secs(3_601)).unwrap_err(),
        PostgresChangeStreamCheckpointError::InvalidLeaseDuration
    ));
    assert_eq!(
        ChangeStreamLeaseDuration::new(Duration::from_millis(10))
            .unwrap()
            .get(),
        Duration::from_millis(10)
    );
}
