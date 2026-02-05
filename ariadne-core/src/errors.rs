use crate::memgraph;
use thiserror::Error;

#[derive(Error, Debug)]
#[error(transparent)]
pub struct AriadneError(Box<ErrorKind>);

#[derive(Error, Debug)]
#[error(transparent)]
pub enum ErrorKind {
    #[error("SerdeJsonError: {0}")]
    SerdeJsonError(#[from] serde_json::Error),
    #[error("IoError: {0}")]
    IoError(#[from] std::io::Error),
    #[error("KubeClientError: {0}")]
    KubeClientError(#[from] kube::Error),
    #[error("KubeconfigError: {0}")]
    KubeconfigError(#[from] kube::config::KubeconfigError),
    #[error("KubeconfigInferError: {0}")]
    KubeconfigInferError(#[from] kube::config::InClusterError),
    #[error("MemgraphError: {0}")]
    MemgraphError(#[from] memgraph::MemgraphError),
    #[error("InvalidResourceTypeError: {0}")]
    InvalidResourceTypeError(String),
}

impl<E> From<E> for AriadneError
where
    ErrorKind: From<E>,
{
    fn from(err: E) -> Self {
        AriadneError(Box::new(ErrorKind::from(err)))
    }
}
