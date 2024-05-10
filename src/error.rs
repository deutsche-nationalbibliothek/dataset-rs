pub(crate) type DatasetResult<T> = Result<T, DatasetError>;

#[derive(Debug, thiserror::Error)]
pub(crate) enum DatasetError {
    #[error(transparent)]
    IO(#[from] std::io::Error),

    #[error(transparent)]
    Toml(#[from] toml::de::Error),

    #[error(transparent)]
    Polars(#[from] polars::error::PolarsError),

    #[error("remote error: {0}")]
    Remote(String),

    #[error("{0}")]
    Other(String),
}
