use rustee_tenant::TenantContext;
use sqlx::{Error, PgPool, Postgres, Transaction};

/// `PostgreSQL` custom setting read by tenant row-level-security policies.
pub const POSTGRES_TENANT_SETTING: &str = "rustee.tenant_id";

/// Begins a `PostgreSQL` transaction scoped to one trusted tenant.
///
/// The helper sets [`POSTGRES_TENANT_SETTING`] with `PostgreSQL`'s transaction-local
/// `set_config(..., true)`. Tables must have an application-owned row-level-security policy that
/// compares their tenant column to `current_setting('rustee.tenant_id', true)`; Rustee never
/// enables or migrates RLS policies automatically. The setting is cleared on commit or rollback.
///
/// # Errors
///
/// Returns the driver error when the transaction cannot begin or `PostgreSQL` rejects the scoped
/// setting.
pub async fn begin_tenant_transaction<'a>(
    pool: &'a PgPool,
    tenant: &TenantContext,
) -> Result<Transaction<'a, Postgres>, Error> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT set_config($1, $2, true)")
        .bind(POSTGRES_TENANT_SETTING)
        .bind(tenant.tenant())
        .execute(&mut *transaction)
        .await?;
    Ok(transaction)
}
