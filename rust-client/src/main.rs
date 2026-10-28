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
    collections::{HashMap, HashSet},
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

const PROTOCOL: u64 = 2;
const MAX_FRAME: usize = 4 * 1024 * 1024;
const CALL_WAIT: Duration = Duration::from_secs(1860);
const CLIENT_VERSION: &str = env!("CARGO_PKG_VERSION");
const REACQUIRE_WARNING: &str = "control was lost or expired; call connect(port, baudrate) explicitly to request control again. Requests are never retried automatically.";
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
    notices: Arc<Mutex<HashMap<String, std::collections::VecDeque<String>>>>,
    port_devices: Arc<Mutex<HashMap<String, String>>>,
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
            notices: Arc::new(Mutex::new(HashMap::new())),
            port_devices: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn daemon(&self) -> Result<Arc<Daemon>, String> {
        let _guard = self.connect_lock.lock().unwrap();
        // End the guard before connection_ended locks the same mutex again.
        let current_client = { self.client.lock().unwrap().clone() };
        if let Some(client) = current_client {
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
                let version = result.get("protocol").unwrap_or(&Value::Null);
                return Err(format!("serial-mcp daemon speaks protocol version {version}; this frontend speaks {PROTOCOL}. Stop the old daemon (SIGTERM) and retry."));
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
            self.port_devices.lock().unwrap().clear();
            self.notices.lock().unwrap().clear();
        }
    }

    fn notice(&self, device_id: &str, text: &str) {
        let mut notices = self.notices.lock().unwrap();
        let queue = notices.entry(device_id.to_owned()).or_default();
        if queue.iter().any(|item| item == text) {
            return;
        }
        if queue.len() >= 32 {
            while queue.len() > 30 {
                queue.pop_front();
            }
            if !queue.iter().any(|item| item.contains("notices coalesced")) {
                queue.push_front(
                    "settings notices coalesced; one or more updates were omitted".into(),
                );
            }
        }
        queue.push_back(text.to_owned());
    }

    fn prefix(&self, port: Option<&str>, text: String) -> String {
        let Some(device_id) =
            port.and_then(|port| self.port_devices.lock().unwrap().get(port).cloned())
        else {
            return text;
        };
        let mut notices = self.notices.lock().unwrap();
        let Some(queue) = notices.get_mut(&device_id) else {
            return text;
        };
        if queue.is_empty() {
            return text;
        }
        let prefix = queue
            .drain(..)
            .map(|n| format!("NOTICE: {n}\n"))
            .collect::<String>();
        notices.remove(&device_id);
        format!("{prefix}\n{text}")
    }

    fn drain_events(&self, client: &Arc<Daemon>) -> bool {
        let events = std::mem::take(&mut *client.events.lock().unwrap());
        for event in events {
            let event_name = event.get("event").and_then(Value::as_str).unwrap_or("");
            let device_id = event.get("device_id").and_then(Value::as_str).unwrap_or("");
            match event_name {
                "settings" if !device_id.is_empty() => {
                    let baudrate = event
                        .get("baudrate")
                        .and_then(Value::as_u64)
                        .unwrap_or_default();
                    self.notice(
                        device_id,
                        &format!("settings changed: baudrate is now {baudrate}"),
                    );
                }
                "settings_gap" if !device_id.is_empty() => self.notice(
                    device_id,
                    "settings notices coalesced; one or more updates were omitted",
                ),
                "control_lost" if !device_id.is_empty() => {
                    self.notice(device_id, REACQUIRE_WARNING)
                }
                "event_gap" => {
                    let devices = self
                        .port_devices
                        .lock()
                        .unwrap()
                        .values()
                        .cloned()
                        .collect::<HashSet<_>>();
                    for device_id in devices {
                        self.notice(
                            &device_id,
                            "daemon event queue overflow; some events may be missing",
                        );
                    }
                }
                "shutdown" => {
                    let devices = self
                        .port_devices
                        .lock()
                        .unwrap()
                        .values()
                        .cloned()
                        .collect::<HashSet<_>>();
                    for device_id in devices {
                        self.notice(&device_id, &format!("the serial-mcp daemon is shutting down ({}); the next tool call starts a new one", event.get("reason").and_then(Value::as_str).unwrap_or("unknown")));
                    }
                }
                _ => {}
            }
        }
        !client.closed.load(Ordering::SeqCst)
    }

    fn refusal_text(
        &self,
        what: &str,
        error: &str,
        port: Option<&str>,
        requested_baudrate: Option<u64>,
    ) -> String {
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
        if code == "baudrate_rejected" {
            if let Some(actual) = parsed
                .as_ref()
                .and_then(|v| v.get("baudrate"))
                .and_then(Value::as_u64)
            {
                text.push_str(&format!(
                    " Requested baudrate: {}. Actual baudrate remains {actual}.",
                    requested_baudrate.map_or_else(|| "unknown".into(), |rate| rate.to_string())
                ));
            }
        }
        self.prefix(port, text)
    }

    fn call(&self, name: &str, args: Value) -> Result<String, String> {
        let port = args.get("port").and_then(Value::as_str).map(str::to_owned);
        let requested_baudrate = matches!(name, "connect" | "set_baudrate")
            .then(|| args.get("baudrate").and_then(Value::as_u64))
            .flatten();
        let client = self.daemon()?;
        self.drain_events(&client);
        match client.request(json!({"op":"call","tool":name,"args":args}), CALL_WAIT) {
            Ok(v) => {
                self.drain_events(&client);
                let mut text = v
                    .get("result")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned();
                if name == "set_baudrate" {
                    if let Some(actual) = v.get("baudrate").and_then(Value::as_u64) {
                        text.push_str(&format!(
                            " Baudrate change accepted; actual baudrate is {actual}."
                        ));
                    }
                }
                if name == "connect" {
                    let control = v
                        .get("control")
                        .and_then(Value::as_str)
                        .unwrap_or("not_granted");
                    let actual = v
                        .get("baudrate")
                        .and_then(Value::as_u64)
                        .unwrap_or_default();
                    let requested = requested_baudrate;
                    if control == "not_granted" {
                        text.push_str(&format!(
                            " Control not granted; joined read-only at actual baudrate {actual}."
                        ));
                    } else {
                        text.push_str(" Control granted.");
                    }
                    if let Some(requested) = requested.filter(|requested| *requested != actual) {
                        text.push_str(&format!(" Requested baudrate {requested} differs; the open device remains at {actual}."));
                    }
                    if let Some(remaining) = v.get("lease_remaining").and_then(Value::as_f64) {
                        text.push_str(&format!(" Remaining control lease: {remaining:.1}s."));
                    }
                }
                if let Some(warning) = v.get("warning").and_then(Value::as_str) {
                    text.push_str(&format!("\nWARNING: {warning}"));
                }
                if name == "connect" {
                    if let (Some(port), Some(device_id)) =
                        (port.as_ref(), v.get("device_id").and_then(Value::as_str))
                    {
                        self.port_devices
                            .lock()
                            .unwrap()
                            .insert(port.clone(), device_id.to_owned());
                    }
                }
                let text = self.prefix(port.as_deref(), text);
                if name == "disconnect" {
                    if let Some(port) = port.as_ref() {
                        self.port_devices.lock().unwrap().remove(port);
                    }
                }
                Ok(text)
            }
            Err(e) if e.starts_with("daemon error:") => {
                self.drain_events(&client);
                Err(self.refusal_text(name, &e, port.as_deref(), requested_baudrate))
            }
            Err(e) => {
                self.connection_ended(&client);
                Err(self.prefix(port.as_deref(), format!("the connection to the serial-mcp daemon ended while {name} was running ({e}). The device may or may not have acted on it. The call was not retried; check the device state before calling again.")))
            }
        }
    }

    fn close(&self) {
        if let Some(client) = self.client.lock().unwrap().take() {
            client.close();
        }
    }
}

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
                    if !frame.is_object() {
                        break;
                    }
                    if frame.get("v").and_then(Value::as_u64) != Some(PROTOCOL) {
                        let version = frame.get("v").unwrap_or(&Value::Null);
                        let rejected = frame
                            .get("error")
                            .and_then(|error| error.get("code"))
                            .and_then(Value::as_str)
                            == Some("unsupported_version");
                        let detail = frame
                            .get("error")
                            .and_then(|error| error.get("message"))
                            .and_then(Value::as_str);
                        let message = if rejected {
                            format!(
                                "daemon protocol mismatch: this frontend speaks protocol {PROTOCOL}; the daemon rejected that version{}",
                                detail.map(|text| format!(": {text}")).unwrap_or_default()
                            )
                        } else {
                            format!(
                                "daemon protocol mismatch: daemon sent protocol version {version}; this frontend speaks protocol {PROTOCOL}"
                            )
                        };
                        if let Some(this) = weak.upgrade() {
                            let mut pending = this.pending.lock().unwrap();
                            if let Some(id) = frame.get("id").and_then(Value::as_u64) {
                                if let Some(tx) = pending.remove(&id) {
                                    let _ = tx.send(Err(message));
                                }
                            } else if rejected {
                                for (_, tx) in pending.drain() {
                                    let _ = tx.send(Err(message.clone()));
                                }
                            }
                        }
                        break;
                    }
                    let unsupported = frame.get("error").and_then(|error| error.get("code")).and_then(Value::as_str) == Some("unsupported_version");
                    if unsupported {
                        if let Some(this) = weak.upgrade() {
                            let message = format!("daemon protocol mismatch: this frontend speaks protocol {PROTOCOL}; the daemon rejected that version: {}", frame["error"]["message"].as_str().unwrap_or("unsupported protocol"));
                            for (_, tx) in this.pending.lock().unwrap().drain() { let _ = tx.send(Err(message.clone())); }
                        }
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
                        if events.len() >= 63 {
                            if let Some(gap) = events.iter_mut().find(|event| event["event"] == "event_gap") {
                                let dropped = gap["dropped_events"].as_u64().unwrap_or(1) + 1;
                                gap["dropped_events"] = json!(dropped);
                            } else {
                                if events.len() >= 63 { events.remove(0); }
                                events.push(json!({"event":"event_gap","dropped_events":1}));
                            }
                            if events.len() >= 64 { events.remove(0); }
                        }
                        events.push(frame);
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

fn daemon_module_dir() -> Option<PathBuf> {
    let executable = std::env::current_exe().ok()?.canonicalize().ok()?;
    executable
        .ancestors()
        .filter(|path| path.is_dir())
        .find(|path| path.join("serial_mcp_daemon.py").is_file())
        .map(Path::to_path_buf)
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
    let lock_path = dir.join("daemon.start.lock");
    let started = Instant::now();
    let mut child: Option<Child> = None;
    let mut spawn_lock: Option<fs::File> = None;
    let mut spawns = 0;
    loop {
        let child_exited = if let Some(child_proc) = child.as_mut() {
            child_proc.try_wait().map_err(|e| e.to_string())?.is_some()
        } else {
            false
        };
        if child_exited {
            child = None;
            spawn_lock = None;
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
            if spawn_lock.is_none() {
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
                    // Hold the guard until the socket is reachable; dropping it after spawn
                    // lets concurrent frontends launch duplicate daemons during startup.
                    spawn_lock = Some(probe);
                }
            }
            if spawn_lock.is_some() {
                let mut cmd =
                    Command::new(std::env::var("PYTHON").unwrap_or_else(|_| "python3".into()));
                cmd.args(["-m", "serial_mcp_daemon"])
                    .env("SERIAL_MCP_RUNTIME_DIR", &dir)
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .current_dir(
                        daemon_module_dir()
                            .or_else(|| std::env::current_dir().ok())
                            .unwrap_or_default(),
                    );
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
    // Keep the MCP server available even if daemon startup fails, but register
    // healthy clients before they become idle without invoking a tool.
    let registration = frontend.clone();
    thread::spawn(move || {
        let _ = registration.daemon();
    });
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
    fn bundled_catalog_matches_the_shared_daemon_v2_surface_and_validates_arguments() {
        let tools: Vec<Value> = serde_json::from_str(CATALOG).unwrap();
        assert_eq!(tools.len(), 16);
        let names: Vec<_> = tools
            .iter()
            .filter_map(|tool| tool["name"].as_str())
            .collect();
        for expected in ["connect", "upload_file", "download_file", "set_baudrate"] {
            assert!(names.contains(&expected));
        }
        for removed in [
            "attach",
            "tio_info",
            "lease_acquire",
            "lease_release",
            "lease_status",
        ] {
            assert!(!names.contains(&removed));
        }
        for item in &tools {
            let name = item["name"].as_str().unwrap();
            if [
                "connect",
                "disconnect",
                "send_command",
                "read_output",
                "clear_output",
                "set_prompt_pattern",
                "login",
                "enter_bootloader",
                "upload_file",
                "download_file",
                "view_io",
                "set_baudrate",
            ]
            .contains(&name)
            {
                assert!(item["inputSchema"]["required"]
                    .as_array()
                    .unwrap()
                    .contains(&json!("port")));
            }
        }
        let send = tools
            .iter()
            .find(|tool| tool["name"] == "send_command")
            .unwrap();
        assert!(validate_args(
            &send["inputSchema"],
            &json!({"port":"/dev/fake", "text":"abc"})
        )
        .is_ok());
        assert!(validate_args(&send["inputSchema"], &json!({}))
            .unwrap_err()
            .contains("port"));
        assert!(validate_args(
            &send["inputSchema"],
            &json!({"port":"/dev/fake", "text":42})
        )
        .unwrap_err()
        .contains("wrong type"));
        assert!(validate_args(
            &send["inputSchema"],
            &json!({"port":"/dev/fake", "text":"x", "extra":true})
        )
        .unwrap_err()
        .contains("unexpected"));
        let bootloader = tools
            .iter()
            .find(|tool| tool["name"] == "enter_bootloader")
            .unwrap();
        assert!(validate_args(
            &bootloader["inputSchema"],
            &json!({"port":"/dev/fake", "interrupt_keys":[" "]})
        )
        .is_ok());
        assert!(validate_args(
            &bootloader["inputSchema"],
            &json!({"port":"/dev/fake", "interrupt_keys":[7]})
        )
        .unwrap_err()
        .contains("interrupt_keys"));
    }

    #[test]
    fn notices_remain_bounded_and_surface_coalescing_and_settings_gap() {
        let frontend = Frontend::new();
        frontend
            .port_devices
            .lock()
            .unwrap()
            .insert("/dev/synthetic".into(), "device-1".into());
        for index in 0..100 {
            frontend.notice("device-1", &format!("settings update {index}"));
        }
        let queue = frontend.notices.lock().unwrap();
        assert!(queue["device-1"].len() <= 32);
        assert!(queue["device-1"]
            .iter()
            .any(|notice| notice.contains("coalesced")));
        drop(queue);
        let (stream, _peer) = UnixStream::pair().unwrap();
        let daemon = Arc::new(Daemon {
            writer: Mutex::new(stream),
            pending: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            closed: AtomicBool::new(false),
            _child: None,
            events: Mutex::new(vec![json!({
                "event":"settings_gap", "device_id":"device-1"
            })]),
        });
        assert!(frontend.drain_events(&daemon));
        let result = frontend.prefix(Some("/dev/synthetic"), "tool result".into());
        assert!(result.contains("NOTICE: settings notices coalesced"));
        assert!(result.ends_with("tool result"));
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
