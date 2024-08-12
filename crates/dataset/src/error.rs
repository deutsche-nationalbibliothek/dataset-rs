pub(crate) type DatasetResult<T> = Result<T, DatasetError>;

macro_rules! bail {
    ($($arg:tt)*) => {{
        return Err(DatasetError::Other(format!($($arg)*)));
    }};
}

pub(crate) use bail;

#[derive(Debug, thiserror::Error)]
pub(crate) enum DatasetError {
    #[error(transparent)]
    IO(#[from] std::io::Error),

    #[error(transparent)]
    Toml(#[from] toml::de::Error),

    #[error("{0}")]
    Other(String),
}
