use thiserror::Error;
use tokio::task::JoinError;
use std::io;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("WebSocket connection error: {0}")]
    ConnectionError(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("JSON serialization/deserialization error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("HTTP request error: {0}")]
    RequestError(#[from] reqwest::Error),

    #[error("Task join error: {0}")]
    JoinError(#[from] JoinError),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("API Error - Code: {0}, Message: {1}")]
    ApiError(String, String),

    #[error("Application logic error: {0}")]
    LogicError(String),

    #[error("MPSC channel send error (type erased)")]
    MpscSendError,
}

impl From<io::Error> for AppError {
    fn from(e: io::Error) -> Self {
        AppError::ConfigError(e.to_string())
    }
}