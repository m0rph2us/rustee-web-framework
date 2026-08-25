//! Durable usage-ledger fixtures.

use super::*;

#[derive(Clone)]
pub(in crate::tests) struct CapturingUsageLedger {
    pub(in crate::tests) decision: AiUsageReservationDecision,
    pub(in crate::tests) reservations: Arc<Mutex<Vec<AiUsageReservation>>>,
    pub(in crate::tests) settlements: Arc<Mutex<Vec<AiUsageSettlement>>>,
    pub(in crate::tests) fail_settlement: bool,
}

impl AiUsageLedger for CapturingUsageLedger {
    type Error = TestUsageLedgerError;

    fn reserve(
        &self,
        reservation: AiUsageReservation,
    ) -> futures_util::future::BoxFuture<'static, Result<AiUsageReservationDecision, Self::Error>>
    {
        let reservations = Arc::clone(&self.reservations);
        let decision = self.decision;
        Box::pin(async move {
            reservations
                .lock()
                .expect("test usage ledger lock is available")
                .push(reservation);
            Ok(decision)
        })
    }

    fn record_usage(
        &self,
        settlement: AiUsageSettlement,
    ) -> futures_util::future::BoxFuture<'static, Result<(), Self::Error>> {
        let settlements = Arc::clone(&self.settlements);
        let fail_settlement = self.fail_settlement;
        Box::pin(async move {
            if fail_settlement {
                return Err(TestUsageLedgerError::Unavailable);
            }
            settlements
                .lock()
                .expect("test usage ledger lock is available")
                .push(settlement);
            Ok(())
        })
    }
}

pub(in crate::tests) fn usage_ledger(
    decision: AiUsageReservationDecision,
    fail_settlement: bool,
) -> CapturingUsageLedger {
    CapturingUsageLedger {
        decision,
        reservations: Arc::new(Mutex::new(Vec::new())),
        settlements: Arc::new(Mutex::new(Vec::new())),
        fail_settlement,
    }
}

pub(in crate::tests) fn usage_reservation(key: &str) -> AiUsageReservation {
    AiUsageReservation::for_request(ai_context(), key, &request())
        .expect("test usage reservation is valid")
}
