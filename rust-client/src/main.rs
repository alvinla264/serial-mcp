use rmcp::service::{RequestContext, RoleServer};
use rmcp::{
    handler::server::router::tool::{ToolRoute, ToolRouter},
    model::{
        CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, Implementation,
        InitializeResult, ServerCapabilities,
    },
    transport::stdio,
    ServerHandler, ServiceExt,
};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    fs,
    io::{BufRead, BufReader, Write},
    os::unix::{
        fs::{DirBuilderExt, MetadataExt, PermissionsExt},
        net::UnixStream,
        process::CommandExt,
    },
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc, Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader as AsyncBufReader};

const PROTOCOL: u64 = 1;
const MAX_FRAME: usize = 4 * 1024 * 1024;
const CALL_WAIT: Duration = Duration::from_secs(1860);
const CLIENT_VERSION: &str = env!("CARGO_PKG_VERSION");
const REACQUIRE_WARNING: &str = "control was lost; device state may have changed since you last used it. Call lease_acquire explicitly before using the device again. Requests are never retried automatically.";
const CATALOG: &str = include_str!("../tools.json");

fn read_daemon_line(
    reader: &mut BufReader<UnixStream>,
    line: &mut Vec<u8>,
) -> std::io::Result<bool> {
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return if line.is_empty() {
                Ok(false)
            } else {
                Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "truncated daemon frame",
                ))
            };
        }
        let end = available
            .iter()
            .position(|b| *b == b'\n')
            .map(|i| i + 1)
            .unwrap_or(available.len());
        if line.len() + end > MAX_FRAME {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "daemon frame exceeds 4 MiB",
            ));
        }
        let complete = available.get(end - 1) == Some(&b'\n');
        line.extend_from_slice(&available[..end]);
        reader.consume(end);
        if complete {
            return Ok(true);
        }
    }
}

fn schema_type_matches(schema: &Value, value: &Value) -> bool {
    if let Some(any) = schema.get("anyOf").and_then(Value::as_array) {
        return any.iter().any(|s| schema_type_matches(s, value));
    }
    match schema.get("type").and_then(Value::as_str) {
        Some("string") => value.is_string(),
        Some("integer") => value.as_i64().is_some() || value.as_u64().is_some(),
        Some("number") => value.is_number(),
        Some("boolean") => value.is_boolean(),
        Some("array") => value.is_array(),
        Some("object") => value.is_object(),
        Some("null") => value.is_null(),
        _ => true,
    }
}

fn validate_value(schema: &Value, value: &Value, path: &str) -> Result<(), String> {
    if let Some(alternatives) = schema.get("anyOf").and_then(Value::as_array) {
        if alternatives
            .iter()
            .any(|alternative| validate_value(alternative, value, path).is_ok())
        {
            return Ok(());
        }
        return Err(format!("argument '{path}' does not match any allowed type"));
    }
    if !schema_type_matches(schema, value) {
        return Err(format!("argument '{path}' has the wrong type"));
    }
    if let Some(object) = value.as_object() {
        let props = schema.get("properties").and_then(Value::as_object);
        if let Some(required) = schema.get("required").and_then(Value::as_array) {
            for name in required.iter().filter_map(Value::as_str) {
                if !object.contains_key(name) {
                    return Err(format!("missing required argument '{path}.{name}'"));
                }
            }
        }
        for (name, nested) in object {
            match props.and_then(|properties| properties.get(name)) {
                Some(property) => validate_value(property, nested, &format!("{path}.{name}"))?,
                None if schema.get("additionalProperties") == Some(&Value::Bool(false)) => {
                    return Err(format!("unexpected argument '{path}.{name}'"));
                }
                None => {}
            }
        }
    }
    if let (Some(items), Some(array)) = (schema.get("items"), value.as_array()) {
        for (index, item) in array.iter().enumerate() {
            validate_value(items, item, &format!("{path}[{index}]"))?;
        }
    }
    Ok(())
}

fn validate_args(schema: &Value, args: &Value) -> Result<(), String> {
    if !args.is_object() {
        return Err("arguments must be an object".into());
    }
    validate_value(schema, args, "arguments")
}

#[derive(Clone)]
struct Frontend {
    client: Arc<Mutex<Option<Arc<Daemon>>>>,
    connect_lock: Arc<Mutex<()>>,
    held: Arc<AtomicBool>,
    reacquire: Arc<AtomicBool>,
    notices: Arc<Mutex<Vec<String>>>,
}

struct Daemon {
    writer: Mutex<UnixStream>,
    pending: Mutex<HashMap<u64, mpsc::Sender<Result<Value, String>>>>,
    next_id: AtomicU64,
    closed: AtomicBool,
    _child: Option<Mutex<Child>>,
    events: Mutex<Vec<Value>>,
}

impl Frontend {
    fn new() -> Self {
        Self {
            client: Arc::new(Mutex::new(None)),
            connect_lock: Arc::new(Mutex::new(())),
            held: Arc::new(AtomicBool::new(false)),
            reacquire: Arc::new(AtomicBool::new(false)),
            notices: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn daemon(&self) -> Result<Arc<Daemon>, String> {
        let _guard = self.connect_lock.lock().unwrap();
        if let Some(client) = self.client.lock().unwrap().clone() {
            if !client.closed.load(Ordering::SeqCst) {
                return Ok(client);
            }
            self.connection_ended(&client);
        }
        for attempt in 0..3 {
            let client = ensure_daemon()?;
            let result = client.request(
                json!({"op":"hello","role":"mcp","name":client_name(),
                "pid":std::process::id(),"client_version":CLIENT_VERSION}),
                Duration::from_secs(5),
            );
            let result = match result {
                Ok(result) => result,
                Err(error) if error.contains("shutting_down") && attempt < 2 => {
                    client.close();
                    thread::sleep(Duration::from_millis(200));
                    continue;
                }
                Err(error) => return Err(format!("serial-mcp daemon handshake failed: {error}")),
            };
            if result.get("protocol").and_then(Value::as_u64) != Some(PROTOCOL) {
                return Err(format!("serial-mcp daemon speaks protocol {:?}; this frontend speaks {PROTOCOL}. Stop the old daemon (SIGTERM) and retry.", result.get("protocol")));
            }
            *self.client.lock().unwrap() = Some(client.clone());
            return Ok(client);
        }
        Err("the serial-mcp daemon kept shutting down during registration; try again".into())
    }

    fn connection_ended(&self, client: &Arc<Daemon>) {
        let mut slot = self.client.lock().unwrap();
        if slot.as_ref().is_some_and(|c| Arc::ptr_eq(c, client)) {
            *slot = None;
            if self.held.swap(false, Ordering::SeqCst) {
                self.reacquire.store(true, Ordering::SeqCst);
                self.notice(REACQUIRE_WARNING);
            }
        }
    }

    fn notice(&self, text: &str) {
        let mut n = self.notices.lock().unwrap();
        if n.len() < 20 && !n.iter().any(|x| x == text) {
            n.push(text.to_owned());
        }
    }

    fn prefix(&self, text: String) -> String {
        let mut notices = self.notices.lock().unwrap();
        if notices.is_empty() {
            return text;
        }
        let prefix = notices
            .drain(..)
            .map(|n| format!("NOTICE: {n}\n"))
            .collect::<String>();
        format!("{prefix}\n{text}")
    }

    fn drain_events(&self, client: &Arc<Daemon>) -> bool {
        let events = std::mem::take(&mut *client.events.lock().unwrap());
        for event in events {
            match event.get("event").and_then(Value::as_str) {
                Some("lease_lost") => {
                    self.held.store(false, Ordering::SeqCst);
                    self.reacquire.store(true, Ordering::SeqCst);
                    self.notice(event.get("warning").and_then(Value::as_str).unwrap_or(REACQUIRE_WARNING));
                }
                Some("shutdown") => self.notice(&format!("the serial-mcp daemon is shutting down ({}); the next tool call starts a new one", event.get("reason").and_then(Value::as_str).unwrap_or("unknown"))),
                _ => {}
            }
        }
        !client.closed.load(Ordering::SeqCst)
    }

    fn refusal_text(&self, what: &str, error: &str) -> String {
        let parsed = error
            .strip_prefix("daemon error:")
            .and_then(|raw| serde_json::from_str::<Value>(raw).ok());
        let code = parsed
            .as_ref()
            .and_then(|v| v.get("code"))
            .and_then(Value::as_str)
            .unwrap_or("internal");
        let message = parsed
            .as_ref()
            .and_then(|v| v.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("request failed");
        let mut text = format!("{what} was refused by the serial-mcp daemon ({code}): {message}");
        if let Some(lease) = parsed.as_ref().and_then(|v| v.get("lease")) {
            text.push_str(&format!(
                " Lease: {}",
                serde_json::to_string(lease).unwrap_or_default()
            ));
        }
        if code == "lease_lost" {
            text.push_str(&format!(" {REACQUIRE_WARNING}"));
        }
        self.prefix(text)
    }

    fn call(&self, name: &str, args: Value) -> Result<String, String> {
        let client = self.daemon()?;
        self.drain_events(&client);
        if GATED.contains(&name) && self.reacquire.load(Ordering::SeqCst) {
            return Err(self.prefix(format!("{name} refused: lease_lost. {REACQUIRE_WARNING}")));
        }
        match client.request(json!({"op":"call","tool":name,"args":args}), CALL_WAIT) {
            Ok(v) => {
                self.drain_events(&client);
                if GATED.contains(&name) && !self.reacquire.load(Ordering::SeqCst) {
                    self.held.store(true, Ordering::SeqCst);
                }
                Ok(self.prefix(
                    v.get("result")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_owned(),
                ))
            }
            Err(e) if e.starts_with("daemon error:") => {
                self.drain_events(&client);
                Err(self.refusal_text(name, &e))
            }
            Err(e) => {
                if GATED.contains(&name) {
                    self.held.store(true, Ordering::SeqCst);
                    self.reacquire.store(true, Ordering::SeqCst);
                    self.notice(REACQUIRE_WARNING);
                    self.held.store(false, Ordering::SeqCst);
                }
                self.connection_ended(&client);
                Err(self.prefix(format!("the connection to the serial-mcp daemon ended while {name} was running ({e}). The device may or may not have acted on it. The call was not retried; check the device state before calling again.")))
            }
        }
    }

    fn close(&self) {
        if let Some(client) = self.client.lock().unwrap().take() {
            client.close();
        }
    }

    fn lease(&self, action: &str) -> Result<String, String> {
        let client = self.daemon()?;
        self.drain_events(&client);
        let mut value = client
            .request(
                json!({"op":"lease","action":action}),
                Duration::from_secs(10),
            )
            .map_err(|e| {
                if e.starts_with("daemon error:") {
                    self.drain_events(&client);
                    self.refusal_text(&format!("lease {action}"), &e)
                } else {
                    self.connection_ended(&client);
                    self.prefix(format!("lease {action} failed: {e}"))
                }
            })?;
        let had_reacquire = self.reacquire.load(Ordering::SeqCst);
        if action == "acquire" {
            self.reacquire.store(false, Ordering::SeqCst);
            self.held.store(true, Ordering::SeqCst);
        }
        if action == "release" {
            self.held.store(false, Ordering::SeqCst);
            self.reacquire.store(true, Ordering::SeqCst);
        }
        if action == "acquire" && had_reacquire && value.get("warning").is_none() {
            value["warning"] = json!(REACQUIRE_WARNING);
        }
        let mut notices = self.notices.lock().unwrap();
        if !notices.is_empty() {
            value["notices"] = json!(notices.drain(..).collect::<Vec<_>>());
        }
        Ok(serde_json::to_string(&value).unwrap_or_else(|_| "{}".into()))
    }
}

const GATED: &[&str] = &[
    "connect",
    "attach",
    "disconnect",
    "send_command",
    "read_output",
    "clear_output",
    "set_prompt_pattern",
    "login",
    "enter_bootloader",
    "upload_file",
    "download_file",
];

impl Daemon {
    fn from_stream(stream: UnixStream, child: Option<Child>) -> Result<Arc<Self>, String> {
        let reader_stream = stream.try_clone().map_err(|e| e.to_string())?;
        let daemon = Arc::new(Self {
            writer: Mutex::new(stream),
            pending: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            closed: AtomicBool::new(false),
            _child: child.map(Mutex::new),
            events: Mutex::new(Vec::new()),
        });
        let weak = Arc::downgrade(&daemon);
        thread::Builder::new()
            .name("serial-mcp-ipc-reader".into())
            .spawn(move || {
                let mut reader = BufReader::new(reader_stream);
                loop {
                    let mut line = Vec::new();
                    match read_daemon_line(&mut reader, &mut line) {
                        Ok(true) => {}
                        _ => break,
                    }
                    let Ok(frame) = serde_json::from_slice::<Value>(&line[..line.len() - 1]) else {
                        break;
                    };
                    if frame.get("v").and_then(Value::as_u64) != Some(PROTOCOL)
                        || !frame.is_object()
                    {
                        break;
                    }
                    let Some(this) = weak.upgrade() else { return };
                    if let Some(id) = frame.get("id").and_then(Value::as_u64) {
                        if let Some(tx) = this.pending.lock().unwrap().remove(&id) {
                            let result = if frame.get("ok").and_then(Value::as_bool) == Some(true) {
                                Ok(frame.get("result").cloned().unwrap_or(Value::Null))
                            } else {
                                Err(format!(
                                    "daemon error:{}",
                                    serde_json::to_string(
                                        frame.get("error").unwrap_or(&Value::Null)
                                    )
                                    .unwrap_or_default()
                                ))
                            };
                            let _ = tx.send(result);
                        }
                    } else if frame.get("event").is_some() {
                        let mut events = this.events.lock().unwrap();
                        if events.len() < 64 {
                            events.push(frame);
                        }
                    }
                }
                if let Some(this) = weak.upgrade() {
                    this.closed.store(true, Ordering::SeqCst);
                    for (_, tx) in this.pending.lock().unwrap().drain() {
                        let _ = tx.send(Err("daemon connection closed; outcome unknown".into()));
                    }
                }
            })
            .map_err(|e| e.to_string())?;
        Ok(daemon)
    }

    fn close(&self) {
        if !self.closed.load(Ordering::SeqCst) {
            let _ = self.request(json!({"op":"bye"}), Duration::from_secs(1));
            self.closed.store(true, Ordering::SeqCst);
            let _ = self
                .writer
                .lock()
                .unwrap()
                .shutdown(std::net::Shutdown::Both);
        }
    }

    fn request(&self, mut frame: Value, timeout: Duration) -> Result<Value, String> {
        if self.closed.load(Ordering::SeqCst) {
            return Err("daemon connection is closed".into());
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        frame["v"] = json!(PROTOCOL);
        frame["id"] = json!(id);
        let bytes = serde_json::to_vec(&frame).map_err(|e| e.to_string())?;
        if bytes.len() + 1 > MAX_FRAME {
            return Err("request exceeds daemon frame limit".into());
        }
        let (tx, rx) = mpsc::channel();
        self.pending.lock().unwrap().insert(id, tx);
        let sent = self
            .writer
            .lock()
            .unwrap()
            .write_all(&[bytes.as_slice(), b"\n"].concat());
        if let Err(e) = sent {
            self.pending.lock().unwrap().remove(&id);
            self.closed.store(true, Ordering::SeqCst);
            return Err(format!("send failed: {e}"));
        }
        match rx.recv_timeout(timeout) {
            Ok(result) => result,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                self.pending.lock().unwrap().remove(&id);
                Err("request timed out; outcome unknown and was not retried".into())
            }
            Err(_) => Err("daemon connection closed; outcome unknown".into()),
        }
    }
}

fn client_name() -> String {
    let name = std::env::var("SERIAL_MCP_CLIENT_NAME")
        .unwrap_or_else(|_| format!("mcp-{}", std::process::id()));
    name.chars().take(64).collect()
}

fn ensure_private_runtime_dir(path: &Path, uid: u32) -> Result<(), String> {
    let mut builder = fs::DirBuilder::new();
    builder.mode(0o700);
    match builder.create(path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => {
            return Err(format!(
                "cannot create runtime directory {}: {e}",
                path.display()
            ))
        }
    }
    let meta = fs::symlink_metadata(path)
        .map_err(|e| format!("cannot inspect runtime directory {}: {e}", path.display()))?;
    if !meta.file_type().is_dir() || meta.uid() != uid || meta.permissions().mode() & 0o077 != 0 {
        return Err(format!(
            "unsafe runtime directory {} (must be a real, private directory owned by this user)",
            path.display()
        ));
    }
    Ok(())
}

fn runtime_dir() -> Result<PathBuf, String> {
    let uid = unsafe { libc::geteuid() };
    let path = if let Ok(p) = std::env::var("SERIAL_MCP_RUNTIME_DIR") {
        PathBuf::from(p)
    } else if let Ok(p) = std::env::var("XDG_RUNTIME_DIR") {
        PathBuf::from(p).join("serial-mcp")
    } else {
        PathBuf::from(format!("/tmp/serial-mcp-{uid}"))
    };
    if !path.is_absolute() {
        return Err("runtime directory must be absolute".into());
    }
    ensure_private_runtime_dir(&path, uid)?;
    Ok(path)
}

fn connect(path: &Path) -> Result<UnixStream, String> {
    let stream = UnixStream::connect(path).map_err(|e| e.to_string())?;
    #[cfg(target_os = "linux")]
    {
        let mut cred: libc::ucred = unsafe { std::mem::zeroed() };
        let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
        let rc = unsafe {
            libc::getsockopt(
                std::os::fd::AsRawFd::as_raw_fd(&stream),
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                &mut cred as *mut _ as *mut libc::c_void,
                &mut len,
            )
        };
        if rc != 0 || cred.uid != unsafe { libc::geteuid() } {
            return Err("daemon peer UID does not match effective UID".into());
        }
    }
    Ok(stream)
}

fn ensure_daemon() -> Result<Arc<Daemon>, String> {
    let dir = runtime_dir()?;
    let sock = dir.join("daemon.sock");
    if sock.as_os_str().as_encoded_bytes().len() >= 108 {
        return Err(format!(
            "daemon socket path is too long: {}",
            sock.display()
        ));
    }
    let lock_path = dir.join("daemon.lock");
    let started = Instant::now();
    let mut child: Option<Child> = None;
    let mut spawns = 0;
    loop {
        if let Some(child_proc) = child.as_mut() {
            if child_proc.try_wait().map_err(|e| e.to_string())?.is_some() {
                child = None;
            }
        }
        if let Ok(stream) = connect(&sock) {
            return Daemon::from_stream(stream, child);
        }
        if started.elapsed() > Duration::from_secs(10) {
            return Err(format!(
                "daemon did not become available. Runtime directory: {}",
                dir.display()
            ));
        }
        if child.is_none() && spawns < 3 {
            let probe = fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(&lock_path)
                .map_err(|e| e.to_string())?;
            let rc = unsafe {
                libc::flock(
                    std::os::fd::AsRawFd::as_raw_fd(&probe),
                    libc::LOCK_EX | libc::LOCK_NB,
                )
            };
            if rc == 0 {
                unsafe {
                    libc::flock(std::os::fd::AsRawFd::as_raw_fd(&probe), libc::LOCK_UN);
                }
                let mut cmd =
                    Command::new(std::env::var("PYTHON").unwrap_or_else(|_| "python3".into()));
                cmd.args(["-m", "serial_mcp_daemon"])
                    .env("SERIAL_MCP_RUNTIME_DIR", &dir)
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .current_dir(std::env::current_dir().unwrap_or_default());
                cmd.process_group(0);
                child = Some(cmd.spawn().map_err(|e| {
                    format!(
                        "cannot start serial-mcp daemon: {e}; install serial-mcp daemon components"
                    )
                })?);
                spawns += 1;
            }
        }
        thread::sleep(Duration::from_millis(40));
    }
}

#[derive(Clone)]
struct McpServer {
    routes: ToolRouter<Self>,
}

impl McpServer {
    fn new(frontend: Frontend) -> Result<Self, String> {
        let catalog: Vec<Value> = serde_json::from_str(CATALOG).map_err(|e| e.to_string())?;
        let mut routes = ToolRouter::new();
        routes.transparent_when_not_found = true;
        for item in catalog {
            let name = item["name"]
                .as_str()
                .ok_or("catalog tool missing name")?
                .to_owned();
            let desc = item["description"].as_str().unwrap_or("").to_owned();
            let schema = item["inputSchema"].clone();
            // rmcp takes a JSON object, not an arbitrary Value.
            let schema_object = serde_json::from_value::<rmcp::model::JsonObject>(schema.clone())
                .map_err(|e| e.to_string())?;
            let attr = rmcp::model::Tool::new(name.clone(), desc, schema_object);
            let f = frontend.clone();
            let tool_name = name.clone();
            let route_schema = schema.clone();
            let route = ToolRoute::new_dyn(attr, move |context| {
                let f = f.clone();
                let tool_name = tool_name.clone();
                let schema = route_schema.clone();
                let args = context
                    .arguments
                    .map(Value::Object)
                    .unwrap_or_else(|| json!({}));
                Box::pin(async move {
                    if let Err(error) = validate_args(&schema, &args) {
                        return Ok(CallToolResponse::from(CallToolResult::error(vec![
                            ContentBlock::text(format!("Invalid arguments: {error}")),
                        ])));
                    }
                    if tool_name.starts_with("lease_") {
                        let action = match tool_name.as_str() {
                            "lease_acquire" => "acquire",
                            "lease_release" => "release",
                            _ => "status",
                        };
                        return match tokio::task::spawn_blocking(move || f.lease(action)).await {
                            Ok(Ok(text)) => {
                                Ok(CallToolResponse::from(CallToolResult::success(vec![
                                    ContentBlock::text(text),
                                ])))
                            }
                            Ok(Err(text)) => {
                                Ok(CallToolResponse::from(CallToolResult::error(vec![
                                    ContentBlock::text(text),
                                ])))
                            }
                            Err(e) => Ok(CallToolResponse::from(CallToolResult::error(vec![
                                ContentBlock::text(e.to_string()),
                            ]))),
                        };
                    }
                    match tokio::task::spawn_blocking(move || f.call(&tool_name, args)).await {
                        Ok(Ok(text)) => Ok(CallToolResponse::from(CallToolResult::success(vec![
                            ContentBlock::text(text),
                        ]))),
                        Ok(Err(text)) => Ok(CallToolResponse::from(CallToolResult::error(vec![
                            ContentBlock::text(text),
                        ]))),
                        Err(e) => Ok(CallToolResponse::from(CallToolResult::error(vec![
                            ContentBlock::text(e.to_string()),
                        ]))),
                    }
                })
            });
            routes.add_route(route);
        }
        Ok(Self { routes })
    }
}

impl ServerHandler for McpServer {
    fn get_info(&self) -> InitializeResult {
        InitializeResult::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("serial-mcp", CLIENT_VERSION))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, rmcp::ErrorData> {
        Ok(CallToolResponse::from(CallToolResult::error(vec![
            ContentBlock::text(format!("Unknown tool: {}", request.name)),
        ])))
    }
}

// The frontend is socket-I/O bound; daemon calls run in spawn_blocking, so a
// single async worker avoids one Tokio worker per host core for every agent.
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if !cfg!(target_os = "linux") {
        eprintln!("serial-mcp Rust frontend currently supports Linux/WSL only");
        std::process::exit(2);
    }
    let frontend = Frontend::new();
    // MCP clients expect registration before the first tool invocation.
    let register = frontend.clone();
    tokio::task::spawn_blocking(move || register.daemon()).await??;
    let server = McpServer::new(frontend.clone())?;
    let routes = server.routes.clone().into_iter().collect::<Vec<_>>();
    let (stdin, _stdout) = stdio();
    let (mcp_writer, bridge_reader) = tokio::io::duplex(64 * 1024);
    let output_bridge = tokio::spawn(async move {
        let mut reader = AsyncBufReader::new(bridge_reader);
        let mut stdout = tokio::io::stdout();
        let mut line = Vec::new();
        loop {
            line.clear();
            let count = reader.read_until(b'\n', &mut line).await?;
            if count == 0 {
                break;
            }
            if let Ok(mut frame) = serde_json::from_slice::<Value>(&line) {
                if let Some(result) = frame.get_mut("result").and_then(Value::as_object_mut) {
                    if result.contains_key("resultType") || result.contains_key("tools") {
                        result.entry("cacheScope").or_insert(json!("private"));
                        result.entry("ttlMs").or_insert(json!(0));
                    }
                }
                line = serde_json::to_vec(&frame)?;
                line.push(b'\n');
            }
            stdout.write_all(&line).await?;
            stdout.flush().await?;
        }
        Ok::<(), std::io::Error>(())
    });
    let mut router = rmcp::handler::server::router::Router::new(server).with_tools(routes);
    router.tool_router.transparent_when_not_found = true;
    let service = router.serve((stdin, mcp_writer)).await?;
    service.waiting().await?;
    output_bridge.await??;
    frontend.close();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bundled_catalog_has_the_python_frontend_tool_set_and_validates_arguments() {
        let tools: Vec<Value> = serde_json::from_str(CATALOG).unwrap();
        assert_eq!(tools.len(), 20);
        let names: Vec<_> = tools
            .iter()
            .filter_map(|tool| tool["name"].as_str())
            .collect();
        for expected in [
            "connect",
            "upload_file",
            "download_file",
            "lease_acquire",
            "lease_release",
            "lease_status",
        ] {
            assert!(names.contains(&expected));
        }
        let send = tools
            .iter()
            .find(|tool| tool["name"] == "send_command")
            .unwrap();
        assert!(validate_args(&send["inputSchema"], &json!({"text":"abc"})).is_ok());
        assert!(validate_args(&send["inputSchema"], &json!({}))
            .unwrap_err()
            .contains("text"));
        assert!(validate_args(&send["inputSchema"], &json!({"text":42}))
            .unwrap_err()
            .contains("wrong type"));
        assert!(
            validate_args(&send["inputSchema"], &json!({"text":"x", "extra":true}))
                .unwrap_err()
                .contains("unexpected")
        );
        let bootloader = tools
            .iter()
            .find(|tool| tool["name"] == "enter_bootloader")
            .unwrap();
        assert!(
            validate_args(&bootloader["inputSchema"], &json!({"interrupt_keys":[" "]})).is_ok()
        );
        assert!(
            validate_args(&bootloader["inputSchema"], &json!({"interrupt_keys":[7]}))
                .unwrap_err()
                .contains("interrupt_keys")
        );
    }

    #[test]
    fn runtime_directory_creation_is_private_and_rejects_symlinks_or_open_modes() {
        let root =
            std::env::temp_dir().join(format!("serial-mcp-runtime-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir(&root).unwrap();
        let actual = root.join("actual");
        let uid = unsafe { libc::geteuid() };
        ensure_private_runtime_dir(&actual, uid).unwrap();
        assert_eq!(
            fs::metadata(&actual).unwrap().permissions().mode() & 0o777,
            0o700
        );

        fs::set_permissions(&actual, fs::Permissions::from_mode(0o755)).unwrap();
        assert!(ensure_private_runtime_dir(&actual, uid).is_err());
        fs::set_permissions(&actual, fs::Permissions::from_mode(0o700)).unwrap();

        let link = root.join("link");
        std::os::unix::fs::symlink(&actual, &link).unwrap();
        assert!(ensure_private_runtime_dir(&link, uid).is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn daemon_line_reader_rejects_oversized_frames_without_unbounded_buffering() {
        let (reader, mut writer) = UnixStream::pair().unwrap();
        let sender = thread::spawn(move || {
            let payload = vec![b'x'; MAX_FRAME + 1];
            let _ = writer.write_all(&payload);
        });
        let mut reader = BufReader::new(reader);
        let mut line = Vec::new();
        let error = read_daemon_line(&mut reader, &mut line).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(line.len() <= MAX_FRAME);
        drop(reader);
        sender.join().unwrap();
    }
}
