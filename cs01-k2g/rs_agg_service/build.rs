use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir_env = std::env::var("OUT_DIR").expect("OUT_DIR environment variable not set");
    let out_dir = PathBuf::from(&out_dir_env);
    eprintln!("build.rs: OUT_DIR: {:?}", out_dir);

    let proto_dir = PathBuf::from("proto");
    let proto_file = proto_dir.join("message.proto");

    eprintln!("build.rs: Proto directory: {:?}", proto_dir);
    eprintln!("build.rs: Proto file: {:?}", proto_file);

    // Ensure the proto directory exists
    if !proto_dir.exists() {
        eprintln!("build.rs: ERROR: Proto directory does not exist: {:?}", proto_dir);
        return Err("Proto directory not found".into());
    }
    if !proto_file.exists() {
        eprintln!("build.rs: ERROR: Proto file does not exist: {:?}", proto_file);
        return Err("Proto file not found".into());
    }

    match prost_build::compile_protos(&[&proto_file], &[&proto_dir]) {
        Ok(_) => eprintln!("build.rs: SUCCESS: Successfully compiled protos."),
        Err(e) => {
            eprintln!("build.rs: ERROR: Failed to compile protos: {}", e);
            return Err(e.into());
        }
    };

    // Verify if message.rs was created
    let generated_file = out_dir.join("message.rs");
    if generated_file.exists() {
        eprintln!("build.rs: INFO: Generated file found: {:?}", generated_file);
        eprintln!("build.rs: --- Content of generated file ---");
        eprintln!("{}", std::fs::read_to_string(&generated_file)?);
        eprintln!("build.rs: ---------------------------------");
    } else {
        eprintln!("build.rs: WARNING: Generated file NOT found: {:?}", generated_file);
    }

    Ok(())
}