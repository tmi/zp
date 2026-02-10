use executorch::Module;
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

    // Single inference
    {
        let model_path = format!("data/{}_single.pte", model_type);
        let mut module = Module::from_file(&model_path).expect("Failed to load single module");

        // Warmup
        module.forward(&[]).expect("Single warmup execution failed");

        let mut latencies = Vec::new();
        for _ in 0..20 {
            let start = Instant::now();
            module.forward(&[]).expect("Single execution failed");
            latencies.push(start.elapsed().as_secs_f64() * 1000.0);
        }

        latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p95 = latencies[(latencies.len() as f64 * 0.95) as usize];
        println!("Single inference P95 (Rust ExecuTorch): {:.4} ms", p95);
    }

    // Batch inference
    {
        let model_path = format!("data/{}_batch.pte", model_type);
        let mut module = Module::from_file(&model_path).expect("Failed to load batch module");

        // Warmup
        module.forward(&[]).expect("Batch warmup execution failed");

        let mut latencies = Vec::new();
        for _ in 0..3 {
            let start = Instant::now();
            module.forward(&[]).expect("Batch execution failed");
            latencies.push(start.elapsed().as_secs_f64() * 1000.0);
        }

        let mean_batch: f64 = latencies.iter().sum::<f64>() / latencies.len() as f64;
        println!("Batch inference mean (Rust ExecuTorch): {:.4} ms", mean_batch);
    }
}
