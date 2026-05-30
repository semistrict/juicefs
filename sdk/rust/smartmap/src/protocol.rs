use std::io::{self, Read, Write};
use std::os::unix::net::UnixStream;

use serde::{Deserialize, Serialize};

use crate::{Error, Result, HUGE_PAGE_SIZE};

pub const CONTROL_EVICT: &str = "evict";
pub const CONTROL_EVICT_ACK: &str = "evict_ack";
pub const CONTROL_PROBE: &str = "probe";
pub const CONTROL_PROBE_ACK: &str = "probe_ack";
pub const CONTROL_WRITE_FAULT: &str = "write_fault";
pub const CONTROL_WRITE_FAULT_ACK: &str = "write_fault_ack";

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Extent {
    pub file_offset: u64,
    pub length: u64,
    pub shm_offset: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Region {
    pub base_host_virt_addr: usize,
    pub size: usize,
    pub offset: u64,
    pub page_size: usize,
}

#[derive(Debug, Serialize)]
pub struct Request {
    pub op: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_id: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub mappings: Vec<Region>,
}

impl Request {
    pub fn open(path: String, size: u64) -> Self {
        Self {
            op: "open_memory_file".into(),
            path: Some(path),
            size: Some(size),
            memory_id: None,
            mappings: Vec::new(),
        }
    }

    pub fn serve_faults(memory_id: String, base: usize, len: usize) -> Self {
        Self {
            op: "serve_memory_faults".into(),
            path: None,
            size: None,
            memory_id: Some(memory_id),
            mappings: vec![Region {
                base_host_virt_addr: base,
                size: len,
                offset: 0,
                page_size: HUGE_PAGE_SIZE as usize,
            }],
        }
    }

    pub fn close(memory_id: String) -> Self {
        Self {
            op: "close_memory_file".into(),
            path: None,
            size: None,
            memory_id: Some(memory_id),
            mappings: Vec::new(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct Response {
    pub ok: bool,
    pub error: Option<String>,
    pub memory_id: Option<String>,
    #[serde(default)]
    pub extents: Vec<Extent>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ControlRange {
    pub file_offset: u64,
    pub length: u64,
    pub shm_offset: u64,
}

#[derive(Debug, Deserialize)]
pub struct ControlMessage {
    #[serde(rename = "type")]
    pub kind: String,
    pub request_id: u64,
    #[serde(default)]
    pub memory_id: String,
    #[serde(default)]
    pub ranges: Vec<ControlRange>,
}

impl ControlMessage {
    pub fn ack(&self, ok: bool, err: Option<io::Error>) -> ControlAck {
        let kind = match self.kind.as_str() {
            CONTROL_PROBE => CONTROL_PROBE_ACK,
            CONTROL_WRITE_FAULT => CONTROL_WRITE_FAULT_ACK,
            _ => CONTROL_EVICT_ACK,
        };
        ControlAck {
            kind: kind.into(),
            request_id: self.request_id,
            ok,
            error: err.map(|e| e.to_string()),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ControlAck {
    #[serde(rename = "type")]
    kind: String,
    request_id: u64,
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

pub fn read_response(stream: UnixStream) -> Result<Response> {
    let resp: Response = serde_json::from_reader(stream)?;
    if !resp.ok {
        return Err(Error::Protocol(resp.error.unwrap_or_else(|| {
            "Smartmap request failed without error text".to_string()
        })));
    }
    Ok(resp)
}

pub fn read_control_message(stream: &mut UnixStream) -> io::Result<ControlMessage> {
    let mut line = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        let n = stream.read(&mut byte)?;
        if n == 0 {
            if line.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "control socket closed",
                ));
            }
            break;
        }
        if byte[0] == b'\n' {
            break;
        }
        line.push(byte[0]);
    }
    serde_json::from_slice(&line).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

pub fn write_control_ack(stream: &mut UnixStream, ack: ControlAck) -> io::Result<()> {
    serde_json::to_writer(&mut *stream, &ack)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    stream.write_all(b"\n")?;
    stream.flush()
}
