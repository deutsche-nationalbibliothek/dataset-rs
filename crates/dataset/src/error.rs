pub(crate) type DatashedResult<T> = Result<T, DatashedError>;

#[derive(Debug, thiserror::Error)]
pub(crate) enum DatashedError {}
