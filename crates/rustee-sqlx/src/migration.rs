use sqlx::{
    PgPool,
    migrate::{MigrateError, Migrator},
};

/// Runs migrations from a deployment-controlled [`Migrator`].
///
/// # Errors
///
/// Returns a migration error when metadata cannot be read, a migration fails, or migration lock
/// acquisition fails. Do not invoke this from HTTP application startup.
pub async fn run_migrations(migrator: &Migrator, pool: &PgPool) -> Result<(), MigrateError> {
    migrator.run(pool).await
}
