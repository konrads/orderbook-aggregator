const SERIALIZE_ANNOTATION: &str = "#[derive(serde::Serialize)]";
const TEST_ANNOTATION: &str = "#[cfg_attr(test, derive(serde::Deserialize))]";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = prost_build::Config::new();
    config.type_attribute("Level", TEST_ANNOTATION);
    config.type_attribute("Summary", TEST_ANNOTATION);
    config.type_attribute("Level", SERIALIZE_ANNOTATION);
    config.type_attribute("Summary", SERIALIZE_ANNOTATION);
    tonic_build::configure().compile_with_config(config, &["orderbook.proto"], &["proto"])?;
    Ok(())
}
