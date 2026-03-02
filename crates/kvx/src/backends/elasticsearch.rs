//! # 📡 THE ELASTICSEARCH BACKEND
//!
//! This module re-exports the split source and sink modules so the public
//! backend API stays stable.
//!
//! 🦆 mandatory duck, as decreed by repository law.

mod elasticsearch_sink;
mod elasticsearch_source;

pub(crate) use elasticsearch_sink::ElasticsearchSink;
pub use elasticsearch_sink::ElasticsearchSinkConfig;
pub(crate) use elasticsearch_source::ElasticsearchSource;
pub use elasticsearch_source::ElasticsearchSourceConfig;
