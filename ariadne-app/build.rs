use shadow_rs::{BuildPattern, ShadowBuilder};

fn main() -> shadow_rs::SdResult<()> {
    let _shadow = ShadowBuilder::builder()
        .build_pattern(BuildPattern::RealTime)
        .build()?;

    Ok(())
}
