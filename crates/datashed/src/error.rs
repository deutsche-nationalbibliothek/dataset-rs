pub(crate) type DatashedResult<T> = Result<T, DatashedError>;

macro_rules! bail {
    ($($arg:tt)*) => {{
        return Err(DatashedError::Other(format!($($arg)*)));
    }};
}

pub(crate) use bail;

#[derive(Debug, thiserror::Error)]
pub(crate) enum DatashedError {
    #[error(transparent)]
    IO(#[from] std::io::Error),

    #[error(transparent)]
    Toml(#[from] toml::de::Error),

    #[error(transparent)]
    Csv(#[from] csv::Error),

    #[error(transparent)]
    Polars(#[from] polars::error::PolarsError),

    #[error(transparent)]
    ReadPica(#[from] pica_record::io::ReadPicaError),

    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),

    #[error(transparent)]
    Minus(#[from] minus::MinusError),

    #[error("{0}")]
    Other(String),
}

impl DatashedError {
    #[inline]
    pub(crate) fn other<T: ToString>(s: T) -> Self {
        Self::Other(s.to_string())
    }
}
