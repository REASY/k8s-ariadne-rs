#[cfg(feature = "build-info")]
use shadow_rs::{BuildPattern, ShadowBuilder};

#[cfg(feature = "build-info")]
fn main() -> shadow_rs::SdResult<()> {
    let _shadow = ShadowBuilder::builder()
        .build_pattern(BuildPattern::Lazy)
        .build()?;

    Ok(())
}

#[cfg(not(feature = "build-info"))]
fn main() {}
