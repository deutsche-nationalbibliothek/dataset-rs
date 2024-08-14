use serde::{Deserialize, Serialize};
use url::Url;

use crate::prelude::*;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Remote {
    pub(crate) url: Url,
    pub(crate) predicate: Option<String>,
}

impl Remote {
    pub(crate) fn new<U: Into<Url>, S: ToString>(
        url: U,
        query: Option<S>,
    ) -> DatasetResult<Self> {
        let url = url.into();
        let scheme = url.scheme();

        if scheme != "http" {
            bail!("unsupported scheme {scheme}");
        }

        Ok(Self {
            url,
            predicate: query.map(|s| s.to_string()),
        })
    }

    pub(crate) fn set_url<U: Into<Url>>(
        &mut self,
        url: U,
    ) -> DatasetResult<()> {
        let url = url.into();
        let scheme = url.scheme();

        if scheme != "http" {
            bail!("unsupported scheme {scheme}");
        }

        self.url = url;

        Ok(())
    }

    pub(crate) fn set_predicate<S: ToString>(&mut self, predicate: S) {
        self.predicate = Some(predicate.to_string());
    }
}
