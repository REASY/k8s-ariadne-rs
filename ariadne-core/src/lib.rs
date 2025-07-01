pub mod errors;

pub type Result<T> = std::result::Result<T, errors::AriadneError>;

pub mod id_gen;
pub mod state;
pub mod state_resolver;
pub mod types;

pub mod prelude {
    pub use super::errors::AriadneError;
    pub use super::Result;
}
