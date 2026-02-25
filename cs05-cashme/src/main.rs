use clap::Parser;
use cashme::{Response};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {}

fn main() {
    let _args = Args::parse();
    let res = Response::new(None, Some("hello-world".to_string()));
    println!("Hello World: {:?}", res);
}
