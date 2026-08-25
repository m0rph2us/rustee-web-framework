use std::convert::Infallible;

use crate::{DeliveryAction, JobContext, JobEnvelope, JobId, JobTraceContext, dispatch};

use super::support::WelcomeEmail;

#[tokio::test]
async fn dispatch_acknowledges_only_after_the_handler_succeeds() {
    let envelope = JobEnvelope::with_metadata(JobId::new(), WelcomeEmail { user_id: 7 }, 123)
        .with_trace_context(
            JobTraceContext::new(
                "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
                None,
            )
            .unwrap(),
        );
    let action = dispatch(
        envelope,
        &|job: WelcomeEmail, context: JobContext| async move {
            assert_eq!(job.user_id, 7);
            assert_eq!(context.attempt(), 1);
            assert_eq!(
                context.trace_context().map(JobTraceContext::traceparent),
                Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
            );
            Ok::<_, Infallible>(())
        },
    )
    .await
    .unwrap();

    assert_eq!(action, DeliveryAction::Acknowledge);
}
