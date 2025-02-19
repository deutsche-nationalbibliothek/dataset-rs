use std::ops::{Deref, DerefMut};

use hashbrown::{HashMap, HashSet};
use pica_record::prelude::*;

use crate::prelude::*;

#[derive(Debug, Default)]
pub(crate) struct MscMap {
    paths: Vec<Path>,
    allow: HashSet<String>,
    map: HashMap<String, String>,
}

impl Deref for MscMap {
    type Target = HashMap<String, String>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl DerefMut for MscMap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.map
    }
}

impl MscMap {
    pub(crate) fn from_config(
        _config: &Config,
    ) -> DatashedResult<Self> {
        let paths = vec![
            r#"045E{ e | E == "i" && H == "dnb" }"#,
            r#"045E{ e | E == "i" && H == "dnb-pa" }"#,
            r#"045E{ e | !E? && !H? }"#,
            r#"045E{ e | E == "m" && H in ["aepsg", "emasg"] }"#,
            r#"045E{ e | E == "a" }"#,
        ];

        let allow = HashSet::from_iter(
            [
                "000", "004", "010", "020", "030", "050", "060", "070",
                "080", "090", "100", "130", "150", "200", "220", "230",
                "290", "300", "310", "320", "330", "333.7", "340",
                "350", "355", "360", "370", "380", "390", "400", "420",
                "430", "439", "440", "450", "460", "470", "480", "490",
                "491.8", "500", "510", "520", "530", "540", "550",
                "560", "570", "580", "590", "600", "610", "620",
                "621.3", "624", "630", "640", "650", "660", "670",
                "690", "700", "710", "720", "730", "740", "741.5",
                "750", "760", "770", "780", "790", "791", "792", "793",
                "796", "800", "810", "820", "830", "839", "840", "850",
                "860", "870", "880", "890", "891.8", "900", "910",
                "914.3", "920", "930", "940", "943", "950", "960",
                "970", "980", "990", "B", "K", "S",
            ]
            .map(String::from),
        );

        Ok(Self {
            paths: paths
                .into_iter()
                .filter_map(|path| Path::new(path).ok())
                .collect(),
            allow,
            ..Default::default()
        })
    }

    pub(crate) fn process_record(&mut self, record: &ByteRecord) {
        if let Some(msc) = self
            .paths
            .iter()
            .flat_map(|path| {
                record
                    .path(path, &Default::default())
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
            })
            .find(|msc| self.allow.get(&msc.to_string()).is_some())
        {
            self.insert(
                record.ppn().unwrap().to_string(),
                msc.to_string(),
            );
        }
    }
}
