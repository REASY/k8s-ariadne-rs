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
    #[error("MemgraphError: {0}")]
    MemgraphError(#[from] memgraph::MemgraphError),
}

impl<E> From<E> for AriadneError
where
    ErrorKind: From<E>,
{
    fn from(err: E) -> Self {
        AriadneError(Box::new(ErrorKind::from(err)))
    }
}
