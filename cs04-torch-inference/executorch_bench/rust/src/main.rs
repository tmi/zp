use executorch::Program;
use std::time::Instant;
use std::env;
use std::fs::File;
use std::io::Read;

fn read_bin(path: &str) -> Vec<f32> {
    let mut file = File::open(path).expect("Could not open file");
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).expect("Error reading file");

    buffer.chunks_exact(4)
        .map(|chunk| f32::from_le_bytes(chunk.try_into().unwrap()))
        .collect()
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <model_type>", args[0]);
        return;
    }
    let model_type = &args[1];
    let model_path = format!("data/{}_single.pte", model_type);
    let input_path = format!("data/example_input_single_{}.bin", model_type);

    let mut program = Program::from_file(&model_path).expect("Failed to load program");
    let mut method = program.load_method("forward").expect("Failed to load method");

    let _input_data = read_bin(&input_path);
    // method.set_input(0, &input_data).expect("Failed to set input");

    // Warmup
    method.execute().expect("Warmup execution failed");

    let mut latencies = Vec::new();
    for _ in 0..20 {
        let start = Instant::now();
        method.execute().expect("Execution failed");
        latencies.push(start.elapsed().as_secs_f64() * 1000.0);
    }

    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p95 = latencies[(latencies.len() as f64 * 0.95) as usize];
    println!("Single inference P95 (Rust ExecuTorch): {:.4} ms", p95);
}
