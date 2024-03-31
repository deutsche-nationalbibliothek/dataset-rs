#[derive(Debug, thiserror::Error)]
pub(crate) enum DatasetError {
    #[error(transparent)]
    IO(#[from] std::io::Error),

    #[error(transparent)]
    Toml(#[from] toml::de::Error),

    #[error(transparent)]
    Polars(#[from] polars::error::PolarsError),

    #[error("{0}")]
    Other(String),
}
