use crate::OpenApiError;

pub(crate) const OPENAPI_VERSION: &str = "3.1.1";
pub(crate) const MAX_METADATA_CHARS: usize = 1_024;
pub(crate) const MAX_SCHEMA_BYTES: usize = 128 * 1_024;

pub(crate) fn validate_metadata(
    value: &str,
    field: &'static str,
) -> std::result::Result<(), OpenApiError> {
    if value.trim().is_empty() || value.chars().count() > MAX_METADATA_CHARS || value.contains('\0')
    {
        return Err(OpenApiError::InvalidMetadata { field });
    }
    Ok(())
}

pub(crate) fn validate_identifier(
    value: &str,
    field: &'static str,
) -> std::result::Result<(), OpenApiError> {
    if value.is_empty()
        || value.chars().count() > MAX_METADATA_CHARS
        || !value.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_')
        })
    {
        return Err(OpenApiError::InvalidIdentifier { field });
    }
    Ok(())
}
