// LumeDB TCP Server
// Accepts client connections and processes JSON commands

use crate::auth::Role;
use crate::engine::{Engine, EngineConfig};
use crate::error::LumeResult;
use crate::query::QueryOptions;
use crate::vector::DistanceMetric;
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio_rustls::rustls::ServerConfig as TlsServerConfig;
use tokio_rustls::TlsAcceptor;
use std::fs;
use std::io::BufReader as StdBufReader;

/// Server configuration
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub data_dir: PathBuf,
    pub use_tls: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            host: "127.0.0.1".to_string(),
            port: 7070,
            data_dir: PathBuf::from("./lumedb_data"),
            use_tls: true,
        }
    }
}

/// Load TLS certificates and private key
fn load_tls_config(config: &ServerConfig) -> LumeResult<TlsServerConfig> {
    let cert_path = config.data_dir.join("cert.pem");
    let key_path = config.data_dir.join("key.pem");

    if !cert_path.exists() || !key_path.exists() {
        println!("📝 Generating self-signed TLS certificates...");
        fs::create_dir_all(&config.data_dir).map_err(|e| crate::error::LumeError::Internal(e.to_string()))?;
        
        let subject_alt_names = vec!["localhost".to_string(), config.host.clone()];
        let cert = rcgen::generate_simple_self_signed(subject_alt_names)
            .map_err(|e| crate::error::LumeError::Internal(format!("Cert generation error: {}", e)))?;
        
        fs::write(&cert_path, cert.cert.pem())
            .map_err(|e| crate::error::LumeError::Internal(e.to_string()))?;
        fs::write(&key_path, cert.signing_key.serialize_pem())
            .map_err(|e| crate::error::LumeError::Internal(e.to_string()))?;
    }

    let cert_file = fs::File::open(&cert_path).map_err(|e| crate::error::LumeError::Internal(e.to_string()))?;
    let mut cert_reader = StdBufReader::new(cert_file);
    let certs: Vec<_> = rustls_pemfile::certs(&mut cert_reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| crate::error::LumeError::Internal(e.to_string()))?;

    let key_file = fs::File::open(&key_path).map_err(|e| crate::error::LumeError::Internal(e.to_string()))?;
    let mut key_reader = StdBufReader::new(key_file);
    let key = rustls_pemfile::private_key(&mut key_reader)
        .map_err(|e| crate::error::LumeError::Internal(e.to_string()))?
        .ok_or_else(|| crate::error::LumeError::Internal("No private key found".to_string()))?;

    let config = TlsServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| crate::error::LumeError::Internal(format!("TLS config error: {}", e)))?;

    Ok(config)
}

/// Start the LumeDB TCP server
pub async fn start_server(config: ServerConfig) -> LumeResult<()> {
    let engine_config = EngineConfig {
        data_dir: config.data_dir.clone(),
        ..Default::default()
    };

    let engine = Arc::new(Engine::new(engine_config)?);
    let addr = format!("{}:{}", config.host, config.port);
    let listener = TcpListener::bind(&addr).await.map_err(|e| {
        crate::error::LumeError::Internal(format!("Failed to bind to {}: {}", addr, e))
    })?;

    let tls_acceptor = if config.use_tls {
        let tls_config = load_tls_config(&config)?;
        Some(TlsAcceptor::from(Arc::new(tls_config)))
    } else {
        None
    };

    println!("🌀 LumeDB server listening on {}", addr);
    if config.use_tls {
        println!("🔒 TLS Encryption enabled");
    }
    println!("   Data directory: {}", config.data_dir.display());
    println!("   Ready to accept connections...\n");

    loop {
        let (socket, peer_addr) = listener.accept().await.map_err(|e| {
            crate::error::LumeError::Internal(format!("Accept error: {}", e))
        })?;

        let engine = Arc::clone(&engine);
        let acceptor = tls_acceptor.clone();
        println!("📡 New connection from {}", peer_addr);

        tokio::spawn(async move {
            let is_tls = acceptor.is_some();
            let (mut reader, mut writer): (BufReader<Box<dyn tokio::io::AsyncRead + Unpin + Send>>, Box<dyn tokio::io::AsyncWrite + Unpin + Send>) = if let Some(acceptor) = acceptor {
                match acceptor.accept(socket).await {
                    Ok(tls_stream) => {
                        let (r, w) = tokio::io::split(tls_stream);
                        (BufReader::new(Box::new(r)), Box::new(w))
                    }
                    Err(e) => {
                        eprintln!("❌ TLS Handshake error from {}: {}", peer_addr, e);
                        return;
                    }
                }
            } else {
                let (r, w) = socket.into_split();
                (BufReader::new(Box::new(r)), Box::new(w))
            };

            let mut line = String::new();
            let mut session_role: Option<Role> = None;

            // Send welcome message
            let welcome = json!({
                "status": "connected",
                "server": "LumeDB",
                "version": "0.1.0",
                "use_tls": is_tls,
                "message": "Welcome to LumeDB! Send JSON commands."
            });
            let _ = writer
                .write_all(format!("{}\n", welcome.to_string()).as_bytes())
                .await;

            loop {
                line.clear();
                match reader.read_line(&mut line).await {
                    Ok(0) => {
                        println!("📤 Connection closed: {}", peer_addr);
                        break;
                    }
                    Ok(_) => {
                        let trimmed = line.trim();
                        if trimmed.is_empty() {
                            continue;
                        }

                        let response = process_command(&engine, trimmed, &mut session_role);
                        let response_str = format!("{}\n", response.to_string());
                        if writer.write_all(response_str.as_bytes()).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        eprintln!("❌ Read error from {}: {}", peer_addr, e);
                        break;
                    }
                }
            }
        });
    }
}

/// Process a JSON command and return a response
fn process_command(engine: &Engine, input: &str, session_role: &mut Option<Role>) -> Value {
    // Parse the command
    let cmd: Value = match serde_json::from_str(input) {
        Ok(v) => v,
        Err(e) => {
            return json!({
                "status": "error",
                "error": format!("Invalid JSON: {}", e)
            });
        }
    };

    let action = cmd.get("action").and_then(|v| v.as_str()).unwrap_or("");
    let collection = cmd.get("collection").and_then(|v| v.as_str()).unwrap_or("");

    // Authorization Check
    let is_authenticated = session_role.is_some();
    let can_read = session_role.as_ref().map(|r| r.can_read()).unwrap_or(false);
    let can_write = session_role.as_ref().map(|r| r.can_write()).unwrap_or(false);
    let can_admin = session_role.as_ref().map(|r| r.can_admin()).unwrap_or(false);

    if action != "authenticate" && action != "ping" {
        if !is_authenticated {
            return json!({"status": "error", "error": "Unauthorized. Please authenticate first."});
        }
        
        match action {
            "createCollection" | "dropCollection" | "createIndex" | "createVectorIndex" => {
                if !can_admin {
                    return json!({"status": "error", "error": "Forbidden. Requires Admin role."});
                }
            }
            "insert" | "insertMany" | "update" | "delete" => {
                // Anyone with write access, but restrict writing to _users to Admin only
                if collection == "_users" && !can_admin {
                    return json!({"status": "error", "error": "Forbidden. Modifying _users requires Admin role."});
                }
                if !can_write {
                    return json!({"status": "error", "error": "Forbidden. Requires ReadWrite role."});
                }
            }
            "find" | "findOne" | "count" | "listCollections" | "listIndexes" | "vectorSearch" | "listVectorIndexes" | "stats" => {
                if collection == "_users" && !can_admin {
                    return json!({"status": "error", "error": "Forbidden. Reading _users requires Admin role."});
                }
                if !can_read {
                    return json!({"status": "error", "error": "Forbidden. Requires ReadOnly role."});
                }
            }
            _ => {}
        }
    }

    match action {
        "authenticate" => {
            let username = cmd.get("username").and_then(|v| v.as_str()).unwrap_or("");
            let password = cmd.get("password").and_then(|v| v.as_str()).unwrap_or("");
            
            let query = json!({"username": username, "password": password});
            match engine.find_one("_users", &query) {
                Ok(Some(user_doc)) => {
                    if let Some(role_str) = user_doc.get_field_value("role").and_then(|v| v.as_str().map(|s| s.to_string())) {
                        if let Some(role) = Role::from_str(&role_str) {
                            *session_role = Some(role);
                            return json!({"status": "ok", "message": "Authentication successful", "role": role_str});
                        }
                    }
                    json!({"status": "error", "error": "Invalid role in user document"})
                }
                Ok(None) => json!({"status": "error", "error": "Invalid credentials"}),
                Err(e) => json!({"status": "error", "error": e.to_string()}),
            }
        }

        // ===== Collection Operations =====
        "createCollection" => {
            match engine.create_collection(collection) {
                Ok(_) => json!({"status": "ok", "message": format!("Collection '{}' created", collection)}),
                Err(e) => json!({"status": "error", "error": e.to_string()}),
            }
        }

        "dropCollection" => {
            match engine.drop_collection(collection) {
                Ok(_) => json!({"status": "ok", "message": format!("Collection '{}' dropped", collection)}),
                Err(e) => json!({"status": "error", "error": e.to_string()}),
            }
        }

        "listCollections" => {
            let collections = engine.list_collections();
            let names: Vec<Value> = collections
                .iter()
                .map(|c| {
                    json!({
                        "name": c.name,
                        "docCount": c.doc_count,
                        "indexCount": c.index_count
                    })
                })
                .collect();
            json!({"status": "ok", "collections": names})
        }

        // ===== Document Operations =====
        "insert" => {
            let data = cmd.get("document").cloned().unwrap_or(json!({}));
            let ttl = cmd.get("ttl").and_then(|v| v.as_u64());
            match engine.insert(collection, data, ttl) {
                Ok(doc) => json!({
                    "status": "ok",
                    "insertedId": doc.id,
                    "document": doc.to_json_clean()
                }),
                Err(e) => json!({"status": "error", "error": e.to_string()}),
            }
        }

        "insertMany" => {
            let docs = cmd
                .get("documents")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            let ttl = cmd.get("ttl").and_then(|v| v.as_u64());
            match engine.insert_many(collection, docs, ttl) {
                Ok(inserted) => {
                    let ids: Vec<String> = inserted.iter().map(|d| d.id.clone()).collect();
                    json!({
                        "status": "ok",
                        "insertedCount": ids.len(),
                        "insertedIds": ids
                    })
                }
                Err(e) => json!({"status": "error", "error": e.to_string()}),
            }
        }

        "find" => {
            let query = cmd.get("query").cloned().unwrap_or(json!({}));
            let options = parse_query_options(&cmd);

            match engine.find(collection, &query, Some(options)) {
                Ok(docs) => {
                    let results: Vec<Value> = docs.iter().map(|d| d.to_json_clean()).collect();
                    json!({
                        "status": "ok",
                        "count": results.len(),
                        "documents": results
                    })
                }
                Err(e) => json!({"status": "error", "error": e.to_string()}),
            }
        }

        "findOne" => {
            let query = cmd.get("query").cloned().unwrap_or(json!({}));
            match engine.find_one(collection, &query) {
                Ok(Some(doc)) => json!({
                    "status": "ok",
                    "document": doc.to_json_clean()
                }),
                Ok(None) => json!({"status": "ok", "document": null}),
                Err(e) => json!({"status": "error", "error": e.to_string()}),
            }
        }

        "update" => {
            let query = cmd.get("query").cloned().unwrap_or(json!({}));
            let update = cmd.get("update").cloned().unwrap_or(json!({}));
            match engine.update(collection, &query, &update) {
                Ok(count) => json!({
                    "status": "ok",
                    "modifiedCount": count
                }),
                Err(e) => json!({"status": "error", "error": e.to_string()}),
            }
        }

        "delete" => {
            let query = cmd.get("query").cloned().unwrap_or(json!({}));
            match engine.delete(collection, &query) {
                Ok(count) => json!({
                    "status": "ok",
                    "deletedCount": count
                }),
                Err(e) => json!({"status": "error", "error": e.to_string()}),
            }
        }

        "count" => {
            let query = cmd.get("query").cloned().unwrap_or(json!({}));
            match engine.count(collection, &query) {
                Ok(count) => json!({"status": "ok", "count": count}),
                Err(e) => json!({"status": "error", "error": e.to_string()}),
            }
        }

        // ===== Index Operations =====
        "createIndex" => {
            let field = cmd.get("field").and_then(|v| v.as_str()).unwrap_or("");
            let unique = cmd.get("unique").and_then(|v| v.as_bool()).unwrap_or(false);
            match engine.create_index(collection, field, unique) {
                Ok(_) => json!({
                    "status": "ok",
                    "message": format!("Index created on '{}.{}'", collection, field)
                }),
                Err(e) => json!({"status": "error", "error": e.to_string()}),
            }
        }

        "listIndexes" => {
            let indexes = engine.list_indexes(collection);
            let idx_list: Vec<Value> = indexes
                .iter()
                .map(|idx| {
                    json!({
                        "name": idx.name,
                        "field": idx.field,
                        "unique": idx.unique
                    })
                })
                .collect();
            json!({"status": "ok", "indexes": idx_list})
        }

        // ===== Vector Search Operations =====
        "createVectorIndex" => {
            let field = cmd.get("field").and_then(|v| v.as_str()).unwrap_or("embedding");
            let dimensions = cmd.get("dimensions").and_then(|v| v.as_u64()).unwrap_or(1536) as usize;
            let metric_str = cmd.get("metric").and_then(|v| v.as_str()).unwrap_or("cosine");
            let metric = match metric_str {
                "euclidean" => DistanceMetric::Euclidean,
                "dotProduct" | "dot" => DistanceMetric::DotProduct,
                _ => DistanceMetric::Cosine,
            };

            match engine.create_vector_index(collection, field, dimensions, metric) {
                Ok(_) => json!({
                    "status": "ok",
                    "message": format!("Vector index created on '{}.{}' ({} dims, {:?})", collection, field, dimensions, metric)
                }),
                Err(e) => json!({"status": "error", "error": e.to_string()}),
            }
        }

        "vectorSearch" => {
            let field = cmd.get("field").and_then(|v| v.as_str()).unwrap_or("embedding");
            let k = cmd.get("k").and_then(|v| v.as_u64()).unwrap_or(5) as usize;
            let query_vector: Vec<f32> = cmd
                .get("vector")
                .and_then(|v| v.as_array())
                .map(|arr| arr.iter().filter_map(|v| v.as_f64().map(|f| f as f32)).collect())
                .unwrap_or_default();

            let filter = cmd.get("filter");

            if query_vector.is_empty() {
                json!({"status": "error", "error": "Missing or empty 'vector' field"})
            } else {
                match engine.vector_search(collection, field, query_vector, k, filter) {
                    Ok(results) => {
                        let docs: Vec<Value> = results
                            .iter()
                            .map(|(doc, vr)| {
                                json!({
                                    "document": doc.to_json_clean(),
                                    "score": vr.score,
                                    "distance": vr.distance
                                })
                            })
                            .collect();
                        json!({
                            "status": "ok",
                            "count": docs.len(),
                            "results": docs
                        })
                    }
                    Err(e) => json!({"status": "error", "error": e.to_string()}),
                }
            }
        }

        "listVectorIndexes" => {
            let indexes = engine.list_vector_indexes(collection);
            let idx_list: Vec<Value> = indexes
                .iter()
                .map(|idx| {
                    json!({
                        "field": idx.field,
                        "dimensions": idx.dimensions,
                        "metric": format!("{:?}", idx.metric)
                    })
                })
                .collect();
            json!({"status": "ok", "vectorIndexes": idx_list})
        }

        // ===== Stats =====
        "stats" => {
            let stats = engine.stats();
            json!({
                "status": "ok",
                "stats": {
                    "totalDocuments": stats.total_documents,
                    "totalCollections": stats.total_collections,
                    "totalIndexes": stats.total_indexes,
                    "totalVectors": stats.total_vectors,
                    "memtableSizeBytes": stats.memtable_size_bytes,
                    "sstableCount": stats.sstable_count,
                    "sstableTotalBytes": stats.sstable_total_bytes,
                    "walSequence": stats.wal_sequence,
                    "activeTransactions": stats.active_transactions
                }
            })
        }

        "ping" => json!({"status": "ok", "message": "pong"}),

        _ => json!({
            "status": "error",
            "error": format!("Unknown action: '{}'", action),
            "available_actions": [
                "authenticate",
                "createCollection", "dropCollection", "listCollections",
                "insert", "insertMany", "find", "findOne", "update", "delete", "count",
                "createIndex", "listIndexes",
                "createVectorIndex", "vectorSearch", "listVectorIndexes",
                "stats", "ping"
            ]
        }),
    }
}

/// Parse query options from a command
fn parse_query_options(cmd: &Value) -> QueryOptions {
    let mut options = QueryOptions::default();

    if let Some(sort) = cmd.get("sort").and_then(|v| v.as_object()) {
        options.sort = Some(
            sort.iter()
                .map(|(field, dir)| {
                    let ascending = dir.as_i64().unwrap_or(1) > 0;
                    (field.clone(), ascending)
                })
                .collect(),
        );
    }

    if let Some(skip) = cmd.get("skip").and_then(|v| v.as_u64()) {
        options.skip = Some(skip as usize);
    }

    if let Some(limit) = cmd.get("limit").and_then(|v| v.as_u64()) {
        options.limit = Some(limit as usize);
    }

    if let Some(projection) = cmd.get("projection").and_then(|v| v.as_array()) {
        options.projection = Some(
            projection
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect(),
        );
    }

    options
}
