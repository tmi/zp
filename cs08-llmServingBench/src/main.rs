use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use futures_util::TryStreamExt;
use serde::{Deserialize, Serialize};
use std::time::Instant;
use tokio::io::AsyncBufReadExt;
use tokio_util::io::StreamReader;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// URL of the server (defaults to http://localhost:11434 for Ollama, http://localhost:8000 for vLLM)
    #[arg(short, long)]
    url: Option<String>,

    /// Model name to use
    #[arg(short, long)]
    model: String,

    /// Prompt to send
    #[arg(short, long)]
    prompt: String,

    /// Serving provider
    #[arg(long, value_enum)]
    provider: Provider,

    /// Extra parameters as JSON string (e.g., '{"options": {"thinking": true}}' for Ollama)
    #[arg(short, long)]
    extra_params: Option<String>,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
enum Provider {
    Ollama,
    Vllm,
}

#[derive(Serialize)]
struct OllamaRequest<'a> {
    model: &'a str,
    prompt: &'a str,
    stream: bool,
    #[serde(flatten)]
    extra: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct OllamaResponse {
    response: Option<String>,
    done: bool,
    eval_count: Option<usize>,
}

#[derive(Serialize)]
struct VllmRequest<'a> {
    model: &'a str,
    prompt: &'a str,
    stream: bool,
    #[serde(flatten)]
    extra: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct VllmStreamResponse {
    choices: Vec<VllmChoice>,
    usage: Option<VllmUsage>,
}

#[derive(Deserialize)]
struct VllmChoice {
    text: String,
}

#[derive(Deserialize)]
struct VllmUsage {
    completion_tokens: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let extra_params: Option<serde_json::Value> = if let Some(params_str) = &args.extra_params {
        Some(serde_json::from_str(params_str).context("Failed to parse extra-params as JSON")?)
    } else {
        None
    };

    let url = args.url.clone().unwrap_or_else(|| match args.provider {
        Provider::Ollama => "http://localhost:11434".to_string(),
        Provider::Vllm => "http://localhost:8000".to_string(),
    });

    println!("Benchmarking {} on {} provider at {}...", args.model, format!("{:?}", args.provider).to_lowercase(), url);

    match args.provider {
        Provider::Ollama => benchmark_ollama(&args, &url, extra_params).await?,
        Provider::Vllm => benchmark_vllm(&args, &url, extra_params).await?,
    }

    Ok(())
}

async fn benchmark_ollama(args: &Args, url: &str, extra: Option<serde_json::Value>) -> Result<()> {
    let client = reqwest::Client::new();
    let endpoint = format!("{}/api/generate", url.trim_end_matches('/'));

    let request_body = OllamaRequest {
        model: &args.model,
        prompt: &args.prompt,
        stream: true,
        extra,
    };

    let start_time = Instant::now();
    let response = client.post(&endpoint)
        .json(&request_body)
        .send()
        .await?;

    if !response.status().is_success() {
        let err_text = response.text().await?;
        return Err(anyhow::anyhow!("Ollama API error: {}", err_text));
    }

    let stream = response
        .bytes_stream()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));

    let reader = StreamReader::new(stream);
    let mut lines = reader.lines();

    let mut ttft = None;
    let mut total_tokens = 0;
    let mut server_reported_tokens = None;

    while let Some(line) = lines.next_line().await? {
        if line.is_empty() { continue; }

        let resp: OllamaResponse = serde_json::from_str(&line)
            .with_context(|| format!("Failed to parse Ollama JSON: {}", line))?;

        if ttft.is_none() {
            ttft = Some(start_time.elapsed());
        }

        if let Some(text) = resp.response {
            if !text.is_empty() {
                total_tokens += 1;
            }
        }

        if let Some(eval_count) = resp.eval_count {
            server_reported_tokens = Some(eval_count);
        }

        if resp.done {
            break;
        }
    }

    let total_duration = start_time.elapsed();
    print_results(ttft, total_duration, server_reported_tokens.unwrap_or(total_tokens));

    Ok(())
}

async fn benchmark_vllm(args: &Args, url: &str, extra: Option<serde_json::Value>) -> Result<()> {
    let client = reqwest::Client::new();
    let endpoint = format!("{}/v1/completions", url.trim_end_matches('/'));

    let request_body = VllmRequest {
        model: &args.model,
        prompt: &args.prompt,
        stream: true,
        extra,
    };

    let start_time = Instant::now();
    let response = client.post(&endpoint)
        .json(&request_body)
        .send()
        .await?;

    if !response.status().is_success() {
        let err_text = response.text().await?;
        return Err(anyhow::anyhow!("vLLM API error: {}", err_text));
    }

    let stream = response
        .bytes_stream()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));

    let reader = StreamReader::new(stream);
    let mut lines = reader.lines();

    let mut ttft = None;
    let mut total_tokens = 0;
    let mut server_reported_tokens = None;

    while let Some(line) = lines.next_line().await? {
        let line = line.trim();
        if line.is_empty() { continue; }

        if let Some(json_part) = line.strip_prefix("data: ") {
            let json_part = json_part.trim();
            if json_part == "[DONE]" {
                break;
            }

            let resp: VllmStreamResponse = serde_json::from_str(json_part)
                .with_context(|| format!("Failed to parse vLLM JSON: {}", json_part))?;

            if ttft.is_none() {
                ttft = Some(start_time.elapsed());
            }

            for choice in resp.choices {
                if !choice.text.is_empty() {
                    total_tokens += 1;
                }
            }

            if let Some(usage) = resp.usage {
                server_reported_tokens = Some(usage.completion_tokens);
            }
        }
    }

    let total_duration = start_time.elapsed();
    print_results(ttft, total_duration, server_reported_tokens.unwrap_or(total_tokens));

    Ok(())
}

fn print_results(ttft: Option<std::time::Duration>, total_duration: std::time::Duration, total_tokens: usize) {
    println!("\nBenchmark Results:");
    if let Some(ttft) = ttft {
        println!("Time to First Token (TTFT): {:?}", ttft);
    } else {
        println!("Time to First Token (TTFT): N/A");
    }
    println!("Total Duration: {:?}", total_duration);
    println!("Total Tokens: {}", total_tokens);

    if total_tokens > 0 {
        let generation_duration = if let Some(ttft) = ttft {
            total_duration.saturating_sub(ttft)
        } else {
            total_duration
        };

        let tps = total_tokens as f64 / generation_duration.as_secs_f64();
        println!("Tokens Per Second (TPS): {:.2}", tps);
    }
}
