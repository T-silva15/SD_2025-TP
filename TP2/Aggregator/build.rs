fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("ocean_validator.proto")?;
    Ok(())
}
