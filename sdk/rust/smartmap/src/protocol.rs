use serde::{Deserialize, Serialize};

use crate::HUGE_PAGE_SIZE;

pub const TYPE_MAP: &str = "map";
pub const TYPE_MAPPED: &str = "mapped";
pub const TYPE_ATTACH: &str = "attach";
pub const TYPE_ATTACHED: &str = "attached";
pub const TYPE_FATAL: &str = "fatal";
pub const TYPE_ACK: &str = "ack";
pub const TYPE_RELEASE: &str = "release";
pub const TYPE_PROBE: &str = "probe";
pub const TYPE_WRITE_FAULT: &str = "write_fault";

pub const FD_MEMORY: &str = "memory";
pub const FD_UFFD: &str = "uffd";

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Extent {
    pub file_offset: u64,
    pub length: u64,
    pub shm_offset: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ControlRange {
    pub file_offset: u64,
    pub length: u64,
    pub shm_offset: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Frame {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub path: String,
    #[serde(skip_serializing_if = "is_zero_u64")]
    pub size: u64,
    #[serde(skip_serializing_if = "is_zero_usize")]
    pub page_size: usize,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub fd: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub extents: Vec<Extent>,
    #[serde(skip_serializing_if = "is_zero_usize")]
    pub base_host_virt_addr: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub ranges: Vec<ControlRange>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub released: Vec<ControlRange>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ok: Option<bool>,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub error: String,
}

impl Frame {
    pub fn map(path: String, size: u64) -> Self {
        Self {
            kind: TYPE_MAP.into(),
            path,
            size,
            ..Self::default()
        }
    }

    pub fn attach(base: usize, size: usize) -> Self {
        Self {
            kind: TYPE_ATTACH.into(),
            size: size as u64,
            page_size: HUGE_PAGE_SIZE as usize,
            fd: FD_UFFD.into(),
            base_host_virt_addr: base,
            ..Self::default()
        }
    }

    pub fn ack(ok: bool, err: Option<String>, released: Vec<ControlRange>) -> Self {
        Self {
            kind: TYPE_ACK.into(),
            ok: Some(ok),
            error: err.unwrap_or_default(),
            released,
            ..Self::default()
        }
    }
}

fn is_zero_u64(v: &u64) -> bool {
    *v == 0
}

fn is_zero_usize(v: &usize) -> bool {
    *v == 0
}
