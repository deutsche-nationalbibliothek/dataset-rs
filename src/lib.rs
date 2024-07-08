//! # Dataset
//!
//! This project provides tools to create an manage datasets. The
//! documents are expected to be in plain text format, whereby most of
//! the underlying functions work on byte strings (slices), which are
//! are only _conventionally_ UTF-8.
//!
//! These tools are developments to support the research work in the
//! Automated Indexing System project of the [German National Library].
//! **The tools are not recommended for productive use; no support is
//! provided.**
//!
//! This project provides the following tools:
//!
//! ## Datapod
//!
//! The `datapod` tool creates and manages a _datapod_ that can later be
//! used in a dataset as an data source. It offers an index of the
//! managed documents, which contains various metrics for determining
//! the quality of a document.
//!
//! ## Contributing
//!
//! All contributors are required to "sign-off" their commits (using
//! `git commit -s`) to indicate that they have agreed to the [Developer
//! Certificate of Origin][DCO].
//!
//! ## License
//!
//! This project is licensed under the terms of the [EUPL v1.2].
//!
//! [German National Library]: https://www.dnb.de
//! [DCO]: https://developercertificate.org/
//! [EUPL v1.2]: https://joinup.ec.europa.eu/collection/eupl/eupl-text-eupl-12
