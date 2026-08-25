use crate::{EventContext, EventEnvelope, EventId, dispatch};

use super::support::OrderPaid;

#[tokio::test]
async fn dispatch_passes_event_metadata_before_a_provider_commit() {
    let envelope =
        EventEnvelope::with_metadata(EventId::new(), OrderPaid { order_id: 7 }, "7", 123)
            .unwrap()
            .with_correlation_id("trace-7")
            .unwrap();

    dispatch(
        envelope,
        &|event: OrderPaid, context: EventContext| async move {
            assert_eq!(event.order_id, 7);
            assert_eq!(context.key(), "7");
            assert_eq!(context.correlation_id(), Some("trace-7"));
            Ok::<_, std::convert::Infallible>(())
        },
    )
    .await
    .unwrap();
}
