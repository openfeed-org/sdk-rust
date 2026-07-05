use std::io::Result;

fn main() -> Result<()> {
    prost_build::compile_protos(
        &[
            "proto/openfeed.proto",
            "proto/openfeed_api.proto",
            "proto/openfeed_instrument.proto",
        ],
        &["proto/"],
    )?;
    Ok(())
}
