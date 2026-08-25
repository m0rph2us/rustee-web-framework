use serde::{Deserialize, Serialize};

use crate::Job;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct WelcomeEmail {
    pub(super) user_id: u64,
}

impl Job for WelcomeEmail {
    const NAME: &'static str = "email.welcome";
    const VERSION: u16 = 1;
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct LargeJob {
    pub(super) body: String,
}

impl Job for LargeJob {
    const NAME: &'static str = "large.job";
    const VERSION: u16 = 1;
}
