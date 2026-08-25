//! Durable AI usage-ledger orchestration and terminal stream settlement.

use std::{error::Error as StdError, time::Instant};

use futures_util::StreamExt;
use tracing::Instrument;

use super::{
    AdvisedUsageLedgerPipelineError, AdvisedUsageLedgerStreamError, AiPipeline,
    UsageLedgerPipelineError, UsageLedgerStreamError, record_outcome,
    stream::stop_after_terminal_event,
    telemetry::{
        ai_operation_span, observe_stream, record_advised_usage_ledger_pipeline_error,
        record_advised_usage_ledger_pipeline_result, record_request_metadata,
        record_usage_ledger_pipeline_error, record_usage_ledger_pipeline_result,
    },
};
use crate::{
    AiAdvisor, AiBudgetRequest, AiEventStream, AiExecutionContext, AiProvider, AiStreamEvent,
    AiUsageLedger, AiUsageReservation, AiUsageReservationDecision, ChatRequest, ChatResponse,
};

impl<P: AiProvider> AiPipeline<P> {
    /// Validates, reserves, completes, and durably records provider usage for one chat request.
    ///
    /// The supplied reservation must describe the exact content-free metadata of `request`.
    /// Only a ledger [`AiUsageReservationDecision::Reserved`] decision permits provider
    /// invocation. If provider completion succeeds but durable usage recording fails, the error
    /// retains the response for application-owned reconciliation and does not retry the provider.
    /// Provider failures leave the reservation pending because Rustee cannot safely infer whether
    /// the provider processed or billed the attempt.
    ///
    /// # Errors
    ///
    /// Returns a policy, reservation, ledger, or provider failure.
    pub async fn complete_with_usage_ledger<L>(
        &self,
        request: ChatRequest,
        reservation: AiUsageReservation,
        ledger: &L,
    ) -> Result<ChatResponse, UsageLedgerPipelineError<P::Error, L::Error>>
    where
        L: AiUsageLedger,
    {
        let span = ai_operation_span("complete", &request);
        let started_at = Instant::now();
        let ledger = ledger.clone();
        let request_span = span.clone();
        let result = async {
            let input_characters = self
                .policy
                .validate_and_measure_input(&request)
                .map_err(UsageLedgerPipelineError::Policy)?;
            record_request_metadata(&request_span, &request, input_characters);
            ensure_reservation_matches(&reservation, &request)?;
            reserve_usage::<P::Error, _>(&ledger, reservation.clone()).await?;
            let response = self
                .provider
                .complete(request)
                .await
                .map_err(UsageLedgerPipelineError::Provider)?;
            if let Err(source) = ledger
                .record_usage(reservation.settlement(response.usage()))
                .await
            {
                return Err(UsageLedgerPipelineError::Settlement { response, source });
            }
            Ok(response)
        }
        .instrument(span.clone())
        .await;
        record_usage_ledger_pipeline_result(&span, started_at, &result);
        result
    }

    /// Validates, reserves, and opens an AI text stream with durable terminal-usage recording.
    ///
    /// The ledger records usage only after an application-visible [`AiStreamEvent::Completed`].
    /// If the provider fails, the response body is dropped, or the stream ends without a terminal
    /// event, the reservation remains pending for reconciliation; Rustee never guesses a refund.
    ///
    /// # Errors
    ///
    /// Returns a policy, reservation, ledger, or provider-open failure before a stream is
    /// returned. A terminal usage-ledger failure is emitted inside the returned stream.
    pub async fn stream_with_usage_ledger<L>(
        &self,
        request: ChatRequest,
        reservation: AiUsageReservation,
        ledger: &L,
    ) -> Result<
        AiEventStream<UsageLedgerStreamError<P::Error, L::Error>>,
        UsageLedgerPipelineError<P::Error, L::Error>,
    >
    where
        L: AiUsageLedger,
    {
        let span = ai_operation_span("stream", &request);
        let started_at = Instant::now();
        let ledger = ledger.clone();
        let reservation_for_stream = reservation.clone();
        let request_span = span.clone();
        let result = async {
            let input_characters = self
                .policy
                .validate_and_measure_input(&request)
                .map_err(UsageLedgerPipelineError::Policy)?;
            record_request_metadata(&request_span, &request, input_characters);
            ensure_reservation_matches(&reservation, &request)?;
            reserve_usage::<P::Error, _>(&ledger, reservation).await?;
            self.provider
                .stream(request)
                .await
                .map_err(UsageLedgerPipelineError::Provider)
        }
        .instrument(span.clone())
        .await;
        match result {
            Ok(stream) => {
                record_outcome(&span, started_at, "opened", false);
                let stream = settle_usage_stream(
                    stop_after_terminal_event(stream),
                    ledger,
                    reservation_for_stream,
                );
                Ok(observe_stream(
                    span,
                    started_at,
                    stop_after_terminal_event(stream),
                ))
            }
            Err(error) => {
                record_usage_ledger_pipeline_error(&span, started_at, &error);
                Err(error)
            }
        }
    }

    /// Runs an advisor and a durable usage ledger around one provider completion.
    ///
    /// The advisor first creates the final request, then Rustee validates it and creates a
    /// reservation from that exact content-free metadata. Actual provider usage is settled before
    /// [`AiAdvisor::after_response`] receives application-visible output, so a response advisor
    /// failure cannot erase a completed provider attempt from the durable ledger.
    ///
    /// # Errors
    ///
    /// Returns advisor, policy, reservation, ledger, or provider failure. A terminal usage write
    /// failure retains the completed response for application-owned reconciliation.
    pub async fn complete_with_advisor_and_usage_ledger<A, L>(
        &self,
        request: ChatRequest,
        advisor: &A,
        context: AiExecutionContext,
        idempotency_key: impl Into<String>,
        ledger: &L,
    ) -> Result<ChatResponse, AdvisedUsageLedgerPipelineError<P::Error, A::Error, L::Error>>
    where
        A: AiAdvisor,
        L: AiUsageLedger,
    {
        let span = ai_operation_span("complete", &request);
        let started_at = Instant::now();
        let idempotency_key = idempotency_key.into();
        let ledger = ledger.clone();
        let request_span = span.clone();
        let result = async {
            let request = advisor
                .before_request(request)
                .await
                .map_err(AdvisedUsageLedgerPipelineError::Advisor)?;
            let input_characters = self
                .policy
                .validate_and_measure_input(&request)
                .map_err(UsageLedgerPipelineError::Policy)
                .map_err(AdvisedUsageLedgerPipelineError::Usage)?;
            record_request_metadata(&request_span, &request, input_characters);
            let reservation = AiUsageReservation::for_request(context, idempotency_key, &request)
                .map_err(AdvisedUsageLedgerPipelineError::ReservationMetadata)?;
            reserve_usage::<P::Error, _>(&ledger, reservation.clone())
                .await
                .map_err(AdvisedUsageLedgerPipelineError::Usage)?;
            let response = self
                .provider
                .complete(request)
                .await
                .map_err(UsageLedgerPipelineError::Provider)
                .map_err(AdvisedUsageLedgerPipelineError::Usage)?;
            if let Err(source) = ledger
                .record_usage(reservation.settlement(response.usage()))
                .await
            {
                return Err(AdvisedUsageLedgerPipelineError::Usage(
                    UsageLedgerPipelineError::Settlement { response, source },
                ));
            }
            advisor
                .after_response(response)
                .await
                .map_err(AdvisedUsageLedgerPipelineError::Advisor)
        }
        .instrument(span.clone())
        .await;
        record_advised_usage_ledger_pipeline_result(&span, started_at, &result);
        result
    }

    /// Runs an advisor and a durable usage ledger around an AI text stream.
    ///
    /// The ledger settles raw provider terminal usage before the advisor transforms the terminal
    /// event. Provider errors, incomplete streams, and dropped response bodies remain pending for
    /// reconciliation instead of being silently released or retried.
    ///
    /// # Errors
    ///
    /// Returns advisor, policy, reservation, ledger, or provider-open failure before a stream is
    /// returned. Provider, ledger, or advisor event failures are emitted inside the stream.
    pub async fn stream_with_advisor_and_usage_ledger<A, L>(
        &self,
        request: ChatRequest,
        advisor: &A,
        context: AiExecutionContext,
        idempotency_key: impl Into<String>,
        ledger: &L,
    ) -> Result<
        AiEventStream<AdvisedUsageLedgerStreamError<P::Error, A::Error, L::Error>>,
        AdvisedUsageLedgerPipelineError<P::Error, A::Error, L::Error>,
    >
    where
        A: AiAdvisor,
        L: AiUsageLedger,
    {
        let span = ai_operation_span("stream", &request);
        let started_at = Instant::now();
        let idempotency_key = idempotency_key.into();
        let ledger = ledger.clone();
        let request_span = span.clone();
        let result = async {
            let request = advisor
                .before_request(request)
                .await
                .map_err(AdvisedUsageLedgerPipelineError::Advisor)?;
            let input_characters = self
                .policy
                .validate_and_measure_input(&request)
                .map_err(UsageLedgerPipelineError::Policy)
                .map_err(AdvisedUsageLedgerPipelineError::Usage)?;
            record_request_metadata(&request_span, &request, input_characters);
            let reservation = AiUsageReservation::for_request(context, idempotency_key, &request)
                .map_err(AdvisedUsageLedgerPipelineError::ReservationMetadata)?;
            reserve_usage::<P::Error, _>(&ledger, reservation.clone())
                .await
                .map_err(AdvisedUsageLedgerPipelineError::Usage)?;
            let stream = self
                .provider
                .stream(request)
                .await
                .map_err(UsageLedgerPipelineError::Provider)
                .map_err(AdvisedUsageLedgerPipelineError::Usage)?;
            Ok::<_, AdvisedUsageLedgerPipelineError<P::Error, A::Error, L::Error>>((
                stream,
                reservation,
            ))
        }
        .instrument(span.clone())
        .await;
        match result {
            Ok((stream, reservation)) => {
                record_outcome(&span, started_at, "opened", false);
                let stream =
                    settle_usage_stream(stop_after_terminal_event(stream), ledger, reservation);
                let advisor = advisor.clone();
                let stream = Box::pin(stream.then(move |event| {
                    let advisor = advisor.clone();
                    async move {
                        let event = event.map_err(AdvisedUsageLedgerStreamError::Usage)?;
                        advisor
                            .on_stream_event(event)
                            .await
                            .map_err(AdvisedUsageLedgerStreamError::Advisor)
                    }
                }));
                Ok(observe_stream(
                    span,
                    started_at,
                    stop_after_terminal_event(stream),
                ))
            }
            Err(error) => {
                record_advised_usage_ledger_pipeline_error(&span, started_at, &error);
                Err(error)
            }
        }
    }
}

fn settle_usage_stream<ProviderError, L>(
    stream: AiEventStream<ProviderError>,
    ledger: L,
    reservation: AiUsageReservation,
) -> AiEventStream<UsageLedgerStreamError<ProviderError, L::Error>>
where
    ProviderError: StdError + Send + Sync + 'static,
    L: AiUsageLedger,
{
    let mut unsettled_reservation = Some(reservation);
    Box::pin(stream.then(move |event| {
        let is_completion = matches!(&event, Ok(AiStreamEvent::Completed(_)));
        let reservation = is_completion
            .then(|| unsettled_reservation.take())
            .flatten();
        let ledger = ledger.clone();
        async move {
            let event = event.map_err(UsageLedgerStreamError::Provider)?;
            let AiStreamEvent::Completed(usage) = event else {
                return Ok(event);
            };
            let Some(reservation) = reservation else {
                return Err(UsageLedgerStreamError::DuplicateCompletion);
            };
            ledger
                .record_usage(reservation.settlement(usage))
                .await
                .map_err(UsageLedgerStreamError::Ledger)?;
            Ok(AiStreamEvent::Completed(usage))
        }
    }))
}

/// Applies the only durable-ledger decision that allows a provider attempt.
async fn reserve_usage<ProviderError, L>(
    ledger: &L,
    reservation: AiUsageReservation,
) -> Result<(), UsageLedgerPipelineError<ProviderError, L::Error>>
where
    L: AiUsageLedger,
{
    match ledger
        .reserve(reservation)
        .await
        .map_err(UsageLedgerPipelineError::Reservation)?
    {
        AiUsageReservationDecision::Reserved => Ok(()),
        AiUsageReservationDecision::Denied => Err(UsageLedgerPipelineError::Denied),
        AiUsageReservationDecision::PendingReconciliation => {
            Err(UsageLedgerPipelineError::PendingReconciliation)
        }
        AiUsageReservationDecision::AlreadySettled => Err(UsageLedgerPipelineError::AlreadySettled),
    }
}

fn ensure_reservation_matches<ProviderError, LedgerError>(
    reservation: &AiUsageReservation,
    request: &ChatRequest,
) -> Result<(), UsageLedgerPipelineError<ProviderError, LedgerError>> {
    (reservation.request() == &AiBudgetRequest::from_request(request))
        .then_some(())
        .ok_or(UsageLedgerPipelineError::ReservationRequestMismatch)
}
