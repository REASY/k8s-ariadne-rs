pub mod errors;

pub type Result<T> = std::result::Result<T, errors::AriadneError>;

pub mod cluster_state;
pub mod id_gen;
pub mod types;

pub mod prelude {
    pub use super::errors::AriadneError;
    pub use super::Result;
}
