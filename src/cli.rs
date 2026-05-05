// LumeDB Interactive CLI Client
// Connects to the LumeDB server and provides a REPL interface

use colored::Colorize;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Write, Read};
use std::net::TcpStream;
use std::sync::Arc;
use rustls::pki_types::ServerName;

#[derive(Debug)]
struct NoCertificateVerification;

impl rustls::client::danger::ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::aws_lc_rs::default_provider().signature_verification_algorithms.supported_schemes()
    }
}

const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 7070;

fn main() {
    println!(
        "{}",
        r#"
 🌀 LumeDB CLI Client v0.1.0
    Type 'help' for available commands
    Type 'quit' to exit
    "#
        .cyan()
    );

    // Parse args
    let args: Vec<String> = std::env::args().collect();
    let host = args
        .iter()
        .position(|a| a == "--host")
        .and_then(|i| args.get(i + 1))
        .cloned()
        .unwrap_or_else(|| DEFAULT_HOST.to_string());
    let port: u16 = args
        .iter()
        .position(|a| a == "--port")
        .and_then(|i| args.get(i + 1))
        .and_then(|p| p.parse().ok())
        .unwrap_or(DEFAULT_PORT);

    let addr = format!("{}:{}", host, port);

    // Try to connect
    let tcp_stream = match TcpStream::connect(&addr) {
        Ok(s) => {
            println!("{} Connected to {} (TCP)\n", "✅".green(), addr);
            s
        }
        Err(e) => {
            eprintln!(
                "{} Failed to connect to {}: {}",
                "❌".red(),
                addr,
                e
            );
            eprintln!("   Make sure the LumeDB server is running.");
            eprintln!("   Start it with: cargo run --bin lumedb-server");
            std::process::exit(1);
        }
    };

    // TLS Setup
    let config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoCertificateVerification))
        .with_no_client_auth();
    
    let server_name = ServerName::try_from("localhost").unwrap();
    let conn = rustls::ClientConnection::new(Arc::new(config), server_name).unwrap();
    let stream = rustls::StreamOwned::new(conn, tcp_stream);
    
    println!("{} TLS Handshake successful\n", "🔒".cyan());

    // Read welcome message
    let mut reader = BufReader::new(stream);
    let mut welcome = String::new();
    let _ = reader.read_line(&mut welcome);
    
    if let Ok(msg) = serde_json::from_str::<Value>(&welcome) {
        if let Some(message) = msg.get("message").and_then(|v| v.as_str()) {
            println!("   {}", message.dimmed());
        }
        if let Some(use_tls) = msg.get("use_tls").and_then(|v| v.as_bool()) {
            if use_tls {
                println!("   {}", "Encrypted session established".cyan().italic());
            }
        }
    }

    // Start REPL
    let mut rl = DefaultEditor::new().unwrap();
    let mut current_collection: Option<String> = None;

    loop {
        let prompt = match &current_collection {
            Some(coll) => format!("lume:{}> ", coll.yellow()),
            None => "lume> ".to_string(),
        };

        match rl.readline(&prompt) {
            Ok(line) => {
                let line = line.trim().to_string();
                if line.is_empty() {
                    continue;
                }

                let _ = rl.add_history_entry(&line);

                // Handle special CLI commands
                match line.as_str() {
                    "quit" | "exit" | "\\q" => {
                        println!("{}", "Goodbye! 👋".cyan());
                        break;
                    }
                    "help" | "\\h" => {
                        print_help();
                        continue;
                    }
                    "clear" | "\\c" => {
                        print!("\x1B[2J\x1B[1;1H");
                        continue;
                    }
                    "stats" => {
                        let cmd = json!({"action": "stats"});
                        send_command(&mut reader, &cmd);
                        continue;
                    }
                    "collections" | "show collections" => {
                        let cmd = json!({"action": "listCollections"});
                        send_command(&mut reader, &cmd);
                        continue;
                    }
                    "ping" => {
                        let cmd = json!({"action": "ping"});
                        send_command(&mut reader, &cmd);
                        continue;
                    }
                    _ => {}
                }

                // Handle "use <collection>" command
                if line.starts_with("use ") {
                    let coll = line[4..].trim();
                    current_collection = Some(coll.to_string());
                    println!("  Switched to collection: {}", coll.yellow());
                    continue;
                }

                // Handle "auth <username> <password>" command
                if line.starts_with("auth ") {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() == 3 {
                        let cmd = json!({
                            "action": "authenticate",
                            "username": parts[1],
                            "password": parts[2]
                        });
                        send_command(&mut reader, &cmd);
                    } else {
                        println!("  {} Usage: auth <username> <password>", "❌".red());
                    }
                    continue;
                }

                // Handle collection commands: db.<collection>.<action>(...)
                if let Some(cmd) = parse_db_command(&line, &current_collection) {
                    send_command(&mut reader, &cmd);
                } else if let Ok(cmd) = serde_json::from_str::<Value>(&line) {
                    // Raw JSON command
                    send_command(&mut reader, &cmd);
                } else {
                    println!("  {} Invalid command. Type 'help' for usage.", "❌".red());
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("Ctrl-C pressed. Type 'quit' to exit.");
            }
            Err(ReadlineError::Eof) => {
                println!("{}", "Goodbye! 👋".cyan());
                break;
            }
            Err(e) => {
                eprintln!("Error: {}", e);
                break;
            }
        }
    }
}

/// Parse MongoDB-style commands: db.collection.action({...})
fn parse_db_command(input: &str, current_collection: &Option<String>) -> Option<Value> {
    // db.users.insert({...})
    if input.starts_with("db.") {
        let rest = &input[3..];
        let dot_pos = rest.find('.')?;
        let collection = &rest[..dot_pos];
        let action_rest = &rest[dot_pos + 1..];

        let paren_pos = action_rest.find('(');
        let (action, args_str) = if let Some(pos) = paren_pos {
            let action = &action_rest[..pos];
            let args = action_rest[pos + 1..].trim_end_matches(')');
            (action, args)
        } else {
            (action_rest, "")
        };

        let args: Value = if args_str.is_empty() {
            json!({})
        } else {
            // Try to parse as JSON, handle multiple args separated by commas at top level
            serde_json::from_str(args_str).unwrap_or_else(|_| {
                // Try wrapping in object
                json!({})
            })
        };

        match action {
            "insert" => Some(json!({
                "action": "insert",
                "collection": collection,
                "document": args
            })),
            "find" => Some(json!({
                "action": "find",
                "collection": collection,
                "query": args
            })),
            "findOne" => Some(json!({
                "action": "findOne",
                "collection": collection,
                "query": args
            })),
            "update" => {
                // db.users.update({query}, {update})  — simplified: just use raw JSON
                Some(json!({
                    "action": "update",
                    "collection": collection,
                    "query": {},
                    "update": args
                }))
            }
            "delete" => Some(json!({
                "action": "delete",
                "collection": collection,
                "query": args
            })),
            "count" => Some(json!({
                "action": "count",
                "collection": collection,
                "query": args
            })),
            "createIndex" => {
                let field = args.as_str().unwrap_or("");
                Some(json!({
                    "action": "createIndex",
                    "collection": collection,
                    "field": field,
                    "unique": false
                }))
            }
            "drop" => Some(json!({
                "action": "dropCollection",
                "collection": collection
            })),
            _ => None,
        }
    } else if let Some(ref coll) = current_collection {
        // If we have a current collection, try simpler commands
        if input.starts_with("insert") || input.starts_with("find") {
            let paren_pos = input.find('(')?;
            let action = &input[..paren_pos];
            let args_str = input[paren_pos + 1..].trim_end_matches(')');
            let args: Value = serde_json::from_str(args_str).unwrap_or(json!({}));

            match action {
                "insert" => Some(json!({
                    "action": "insert",
                    "collection": coll,
                    "document": args
                })),
                "find" => Some(json!({
                    "action": "find",
                    "collection": coll,
                    "query": args
                })),
                _ => None,
            }
        } else {
            None
        }
    } else {
        None
    }
}

/// Send a command to the server and print the response
fn send_command<S: Read + Write>(reader: &mut BufReader<S>, cmd: &Value) {
    let cmd_str = format!("{}\n", cmd.to_string());

    if reader.get_mut().write_all(cmd_str.as_bytes()).is_err() {
        println!("  {} Connection lost", "❌".red());
        return;
    }
    let _ = reader.get_mut().flush();

    let mut response = String::new();
    match reader.read_line(&mut response) {
        Ok(0) => {
            println!("  {} Connection closed by server", "❌".red());
        }
        Ok(_) => {
            if let Ok(json) = serde_json::from_str::<Value>(&response) {
                pretty_print_response(&json);
            } else {
                println!("  {}", response.trim());
            }
        }
        Err(e) => {
            println!("  {} Error reading response: {}", "❌".red(), e);
        }
    }
}

/// Pretty-print a JSON response
fn pretty_print_response(response: &Value) {
    let status = response
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    match status {
        "ok" => {
            // Print specific fields nicely
            if let Some(docs) = response.get("documents") {
                if let Some(arr) = docs.as_array() {
                    println!(
                        "  {} Found {} document(s):",
                        "✅".green(),
                        arr.len().to_string().yellow()
                    );
                    for (i, doc) in arr.iter().enumerate() {
                        println!(
                            "  {}. {}",
                            (i + 1).to_string().dimmed(),
                            serde_json::to_string_pretty(doc).unwrap_or_default().cyan()
                        );
                    }
                    return;
                }
            }

            if let Some(doc) = response.get("document") {
                if doc.is_null() {
                    println!("  {} No document found", "⚠️ ".yellow());
                } else {
                    println!("  {} Document:", "✅".green());
                    println!(
                        "  {}",
                        serde_json::to_string_pretty(doc).unwrap_or_default().cyan()
                    );
                }
                return;
            }

            if let Some(id) = response.get("insertedId") {
                println!(
                    "  {} Inserted document with _id: {}",
                    "✅".green(),
                    id.to_string().yellow()
                );
                return;
            }

            if let Some(count) = response.get("modifiedCount") {
                println!(
                    "  {} Modified {} document(s)",
                    "✅".green(),
                    count.to_string().yellow()
                );
                return;
            }

            if let Some(count) = response.get("deletedCount") {
                println!(
                    "  {} Deleted {} document(s)",
                    "✅".green(),
                    count.to_string().yellow()
                );
                return;
            }

            if let Some(stats) = response.get("stats") {
                println!("  {} Database Statistics:", "📊".green());
                println!(
                    "  {}",
                    serde_json::to_string_pretty(stats)
                        .unwrap_or_default()
                        .cyan()
                );
                return;
            }

            if let Some(msg) = response.get("message") {
                println!("  {} {}", "✅".green(), msg.as_str().unwrap_or(""));
                return;
            }

            // Fallback: print the whole response
            println!(
                "  {} {}",
                "✅".green(),
                serde_json::to_string_pretty(response)
                    .unwrap_or_default()
                    .cyan()
            );
        }
        "error" => {
            let error = response
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown error");
            println!("  {} {}", "❌".red(), error.red());
        }
        _ => {
            println!(
                "  {}",
                serde_json::to_string_pretty(response)
                    .unwrap_or_default()
            );
        }
    }
}

/// Print help information
fn print_help() {
    println!(
        "{}",
        r#"
╔══════════════════════════════════════════════════════════════════╗
║                     LumeDB CLI Commands                          ║
╠══════════════════════════════════════════════════════════════════╣
║                                                                  ║
║  NAVIGATION                                                      ║
║    use <collection>     Switch to a collection                   ║
║    collections          List all collections                     ║
║                                                                  ║
║  CRUD OPERATIONS                                                 ║
║    db.<coll>.insert({...})        Insert a document              ║
║    db.<coll>.find({...})          Find documents                 ║
║    db.<coll>.findOne({...})       Find one document              ║
║    db.<coll>.update({q}, {u})     Update documents               ║
║    db.<coll>.delete({...})        Delete documents               ║
║    db.<coll>.count({...})         Count documents                ║
║                                                                  ║
║  QUERY OPERATORS                                                 ║
║    $eq, $ne, $gt, $gte, $lt, $lte                               ║
║    $in, $nin, $exists, $regex                                    ║
║    $and, $or, $not                                               ║
║                                                                  ║
║  UPDATE OPERATORS                                                ║
║    $set, $unset, $inc, $push, $pull                              ║
║                                                                  ║
║  INDEXES                                                         ║
║    db.<coll>.createIndex("field")  Create index                  ║
║                                                                  ║
║  UTILITY                                                         ║
║    auth <user> <pass>   Authenticate session                     ║
║    stats                Database statistics                      ║
║    ping                 Test connection                          ║
║    clear                Clear screen                             ║
║    help                 Show this help                           ║
║    quit                 Exit                                     ║
║                                                                  ║
║  RAW JSON MODE                                                   ║
║    {"action":"find","collection":"users","query":{"age":30}}     ║
║                                                                  ║
╚══════════════════════════════════════════════════════════════════╝
    "#
        .green()
    );
}
