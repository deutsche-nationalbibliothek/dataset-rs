use std::sync::OnceLock;

use lingua::{Language, LanguageDetector, LanguageDetectorBuilder};

pub(crate) fn language_detector() -> &'static LanguageDetector {
    static DETECTOR: OnceLock<LanguageDetector> = OnceLock::new();
    DETECTOR.get_or_init(|| {
        LanguageDetectorBuilder::from_languages(&[
            Language::German,
            Language::English,
        ])
        .build()
    })
}

pub(crate) fn lang_to_639_2b(language: &Language) -> &'static str {
    match language {
        Language::German => "ger",
        Language::English => "eng",
    }
}
