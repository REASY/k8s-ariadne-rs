use ariadne_core::snapshot::{read_json_from_dir, write_json_to_dir};
#[cfg(feature = "build-info")]
use shadow_rs::shadow;
use std::path::Path;

pub mod errors;
pub mod health;
mod kube_tool;
pub mod logger;
pub mod routes;

use crate::health::{SNAPSHOT_MANIFEST_FILE, SnapshotManifest};
pub use crate::kube_tool::GraphSchemaFormat;

#[cfg(feature = "build-info")]
shadow!(build);

#[cfg(feature = "build-info")]
pub const APP_VERSION: &str = shadow_rs::formatcp!(
    "{} ({} {}), build_env: {}, {}, {}",
    build::PKG_VERSION,
    build::SHORT_COMMIT,
    build::BUILD_TIME,
    build::RUST_VERSION,
    build::RUST_CHANNEL,
    build::CARGO_VERSION
);

#[cfg(not(feature = "build-info"))]
pub const APP_VERSION: &str = env!("CARGO_PKG_VERSION");

pub fn read_snapshot_manifest(dir: &Path) -> errors::Result<Option<SnapshotManifest>> {
    let manifest_path = dir.join(SNAPSHOT_MANIFEST_FILE);
    if !manifest_path.exists() {
        return Ok(None);
    }
    Ok(Some(read_json_from_dir(dir, SNAPSHOT_MANIFEST_FILE)?))
}

pub fn write_snapshot_manifest(dir: &Path, manifest: &SnapshotManifest) -> errors::Result<()> {
    write_json_to_dir(dir, SNAPSHOT_MANIFEST_FILE, manifest)?;
    Ok(())
}
