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
    Csv(#[from] csv::Error),

    #[error(transparent)]
    Polars(#[from] polars::error::PolarsError),

    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),

    #[error(transparent)]
    ReadPica(#[from] pica_record::io::ReadPicaError),

    #[error(transparent)]
    PicaMatcher(#[from] pica_matcher::ParseMatcherError),

    #[error(transparent)]
    PicaPath(#[from] pica_path::ParsePathError),

    #[error(transparent)]
    Toml(#[from] toml::de::Error),

    #[error("{0}")]
    Other(String),
}

impl DatasetError {
    #[inline]
    pub(crate) fn other<T: ToString>(s: T) -> Self {
        Self::Other(s.to_string())
    }
}
