use ariadne_core::errors::AriadneError;
use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Error, Debug)]
#[error(transparent)]
pub struct AppError(Box<ErrorKind>);

#[derive(Error, Debug)]
#[error(transparent)]
pub enum ErrorKind {
    #[error("AriadneError: {0}")]
    Ariadne(#[from] AriadneError),
}

impl<E> From<E> for AppError
where
    ErrorKind: From<E>,
{
    fn from(err: E) -> Self {
        AppError(Box::new(ErrorKind::from(err)))
    }
}

impl AppError {
    fn get_codes(&self) -> (StatusCode, u16) {
        match *self.0 {
            ErrorKind::Ariadne(_) => (StatusCode::BAD_REQUEST, 40001),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct ErrorCode {
    pub code: u16,
    pub message: String,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status_code, code) = self.get_codes();
        let message = self.to_string();
        let body = Json(ErrorCode { code, message });
        (status_code, body).into_response()
    }
}

pub type Result<T> = std::result::Result<T, AppError>;

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;

    fn make_ariadne_error() -> AriadneError {
        AriadneError::from(std::io::Error::other("test error"))
    }

    #[test]
    fn test_app_error_from_ariadne_error() {
        let ariadne_err = make_ariadne_error();
        let app_err = AppError::from(ariadne_err);
        let msg = app_err.to_string();
        assert!(
            msg.contains("test error"),
            "expected message to contain 'test error', got: {msg}"
        );
    }

    #[test]
    fn test_get_codes_returns_bad_request_and_40001() {
        let app_err = AppError::from(make_ariadne_error());
        let (status, code) = app_err.get_codes();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(code, 40001);
    }

    #[tokio::test]
    async fn test_into_response_status_and_body() {
        let app_err = AppError::from(make_ariadne_error());
        let response = app_err.into_response();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body_bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let error_code: ErrorCode = serde_json::from_slice(&body_bytes).unwrap();

        assert_eq!(error_code.code, 40001);
        assert!(
            !error_code.message.is_empty(),
            "expected non-empty error message"
        );
        assert!(
            error_code.message.contains("test error"),
            "expected message to contain 'test error', got: {}",
            error_code.message
        );
    }

    #[test]
    fn test_error_code_serialization_round_trip() {
        let original = ErrorCode {
            code: 40001,
            message: "something went wrong".to_string(),
        };
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: ErrorCode = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.code, original.code);
        assert_eq!(deserialized.message, original.message);
    }
}
