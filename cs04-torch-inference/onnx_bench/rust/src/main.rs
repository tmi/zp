use ort::session::Session;
use ort::value::Value;
use std::time::Instant;
use std::env;
use std::fs::File;
use std::io::Read;

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
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <model_type>", args[0]);
        return Ok(());
    }
    let model_type = &args[1];
    let model_path = format!("data/{}.onnx", model_type);
    let input_path = format!("data/example_input_single_{}.bin", model_type);

    let mut session = Session::builder()?.commit_from_file(model_path)?;

    let input_data = read_bin(&input_path);
    let input_shape: Vec<usize> = match model_type.as_str() {
        "simple" => vec![1, 128],
        "rnn" => vec![1, 10, 32],
        "transformer" => vec![1, 16, 64],
        _ => panic!("Unknown model type"),
    };

    // Use (shape, data) tuple which is supported by ort as OwnedTensorArrayData
    let input_tensor = Value::from_array((input_shape, input_data))?;

    // Warmup
    let _ = session.run(ort::inputs![input_tensor.view()])?;

    let mut latencies = Vec::new();
    for _ in 0..20 {
        let start = Instant::now();
        let _ = session.run(ort::inputs![input_tensor.view()])?;
        latencies.push(start.elapsed().as_secs_f64() * 1000.0);
    }

    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p95 = latencies[(latencies.len() as f64 * 0.95) as usize];
    println!("Single inference P95 (Rust ONNX): {:.4} ms", p95);

    Ok(())
}
