use ort::session::Session;
use ort::value::Value;
use std::time::Instant;
use std::fs::File;
use std::io::Read;
use clap::Parser;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Model type (simple, rnn, transformer)
    model_type: String,

    /// Device/Provider (cpu, cuda, coreml, xnnpack)
    #[arg(long, default_value = "cpu")]
    device: String,

    /// Enable verbose logging
    #[arg(long)]
    debug: bool,
}

fn read_bin(path: &str) -> Vec<f32> {
    let mut file = File::open(path).expect("Could not open file");
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).expect("Error reading file");

    // Convert bytes to f32
    buffer.chunks_exact(4)
        .map(|chunk| f32::from_le_bytes(chunk.try_into().unwrap()))
        .collect()
}

fn main() -> ort::Result<()> {
    let args = Args::parse();

    if args.debug {
        let _ = ort::init()
            .with_name("onnx_bench")
            .commit();
    }

    let model_path = format!("data/{}.onnx", args.model_type);
    let single_input_path = format!("data/example_input_single_{}.bin", args.model_type);
    let batch_input_path = format!("data/example_input_batch_{}.bin", args.model_type);

    let mut builder = Session::builder()?;

    // Set optimization level and threads
    builder = builder
        .with_optimization_level(ort::session::builder::GraphOptimizationLevel::Level3)?
        .with_intra_threads(4)?;

    // Set execution provider
    match args.device.as_str() {
        "cpu" => {
            builder = builder.with_execution_providers([ort::execution_providers::CPUExecutionProvider::default().build().error_on_failure()])?;
        }
        "cuda" => {
            builder = builder.with_execution_providers([ort::execution_providers::CUDAExecutionProvider::default().build().error_on_failure()])?;
        }
        "coreml" => {
            builder = builder.with_execution_providers([ort::execution_providers::CoreMLExecutionProvider::default().build().error_on_failure()])?;
        }
        "xnnpack" => {
            builder = builder.with_execution_providers([ort::execution_providers::XNNPACKExecutionProvider::default().build().error_on_failure()])?;
        }
        _ => {
            panic!("Unknown device: {}. Supported: cpu, cuda, coreml, xnnpack", args.device);
        }
    }

    let mut session = builder.commit_from_file(model_path)?;

    // Single inference
    let single_input_data = read_bin(&single_input_path);
    let single_input_shape: Vec<usize> = match args.model_type.as_str() {
        "simple" => vec![1, 128],
        "rnn" => vec![1, 10, 32],
        "transformer" => vec![1, 16, 64],
        _ => panic!("Unknown model type"),
    };

    let single_input_tensor = Value::from_array((single_input_shape, single_input_data))?;

    // Warmup
    let _ = session.run(ort::inputs![single_input_tensor.view()])?;

    let mut latencies = Vec::new();
    for _ in 0..20 {
        let start = Instant::now();
        let _ = session.run(ort::inputs![single_input_tensor.view()])?;
        latencies.push(start.elapsed().as_secs_f64() * 1000.0);
    }

    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p95 = latencies[(latencies.len() as f64 * 0.95) as usize];
    println!("Single inference P95 (Rust ONNX): {:.4} ms", p95);

    // Batch inference
    let batch_input_data = read_bin(&batch_input_path);
    let batch_input_shape: Vec<usize> = match args.model_type.as_str() {
        "simple" => vec![20, 128],
        "rnn" => vec![20, 10, 32],
        "transformer" => vec![20, 16, 64],
        _ => panic!("Unknown model type"),
    };

    let batch_input_tensor = Value::from_array((batch_input_shape, batch_input_data))?;

    // Warmup
    let _ = session.run(ort::inputs![batch_input_tensor.view()])?;

    let mut batch_latencies = Vec::new();
    for _ in 0..3 {
        let start = Instant::now();
        let _ = session.run(ort::inputs![batch_input_tensor.view()])?;
        batch_latencies.push(start.elapsed().as_secs_f64() * 1000.0);
    }

    let mean_batch: f64 = batch_latencies.iter().sum::<f64>() / batch_latencies.len() as f64;
    println!("Batch inference mean (Rust ONNX): {:.4} ms", mean_batch);

    Ok(())
}
