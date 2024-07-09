pub(crate) type DatapodResult<T> = Result<T, DatapodError>;

macro_rules! bail {
    ($($arg:tt)*) => {{
        return Err(DatapodError::Other(format!($($arg)*)));
    }};
}

pub(crate) use bail;

#[derive(Debug, thiserror::Error)]
pub(crate) enum DatapodError {
    #[error(transparent)]
    IO(#[from] std::io::Error),

    #[error(transparent)]
    Toml(#[from] toml::de::Error),

    #[error("{0}")]
    Other(String),
}
