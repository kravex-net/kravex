//! # 📡 THE ELASTICSEARCH BACKEND
//!
//! This module re-exports the split source and sink modules so the public
//! backend API stays stable.
//!
//! 🦆 mandatory duck, as decreed by repository law.

pub mod config;
mod elasticsearch_sink;
mod elasticsearch_source;

pub use config::{ElasticsearchSinkConfig, ElasticsearchSourceConfig};
pub use elasticsearch_sink::ElasticsearchSink;
pub use elasticsearch_source::ElasticsearchSource;
