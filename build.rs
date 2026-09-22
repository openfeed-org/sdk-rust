use std::io::Result;

fn main() -> Result<()> {
    let mut config = prost_build::Config::new();
    config.bytes(&["."]);  // Use Bytes for all bytes fields
    config.compile_protos(
        &[
            "proto/openfeed.proto",
            "proto/openfeed_api.proto",
            "proto/openfeed_instrument.proto",
        ],
        &["proto/"],
    )?;
    Ok(())
}
