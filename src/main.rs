// LumeDB Server — Main Entry Point
// Starts the TCP server for accepting database connections

use colored::Colorize;
use std::path::PathBuf;
use lumedb::server::{start_server, ServerConfig};

#[tokio::main]
async fn main() {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_target(false)
        .with_thread_ids(true)
        .init();

    let banner = r#"
 ██╗      ██╗   ██╗ ███╗   ███╗ ███████╗ ██████╗  ██████╗ 
 ██║      ██║   ██║ ████╗ ████║ ██╔════╝ ██╔══██╗ ██╔══██╗
 ██║      ██║   ██║ ██╔████╔██║ █████╗   ██║  ██║ ██████╔╝
 ██║      ██║   ██║ ██║╚██╔╝██║ ██╔══╝   ██║  ██║ ██╔══██╗
 ███████╗ ╚██████╔╝ ██║ ╚═╝ ██║ ███████╗ ██████╔╝ ██████╔╝
 ╚══════╝  ╚═════╝  ╚═╝     ╚═╝ ╚══════╝ ╚═════╝  ╚═════╝ 
                                                  v0.1.0
    "#;
    println!("{}", banner.cyan());
    println!("{}", "🌀 LumeDB — High-Performance AI-Native Document Database".bold());
    println!("   ACID Compliant | LSM-Tree Storage | Bloom Filters");
    println!("   LZ4 Compression | B-Tree Indexes | MVCC Transactions\n");

    // Parse command-line args or use defaults
    let args: Vec<String> = std::env::args().collect();

    let data_dir = args
        .iter()
        .position(|a| a == "--data-dir")
        .and_then(|i| args.get(i + 1))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("./lumedb_data"));

    let port: u16 = args
        .iter()
        .position(|a| a == "--port")
        .and_then(|i| args.get(i + 1))
        .and_then(|p| p.parse().ok())
        .unwrap_or(7070);

    let host = args
        .iter()
        .position(|a| a == "--host")
        .and_then(|i| args.get(i + 1))
        .cloned()
        .unwrap_or_else(|| "127.0.0.1".to_string());

    let use_tls = !args.iter().any(|a| a == "--no-tls");

    let config = ServerConfig {
        host,
        port,
        data_dir,
        use_tls,
    };

    if let Err(e) = start_server(config).await {
        eprintln!("❌ Fatal error: {}", e);
        std::process::exit(1);
    }
}
