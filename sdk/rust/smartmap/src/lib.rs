#![cfg(target_os = "linux")]

mod mapping;
mod protocol;
mod uds;
mod uffd;

pub mod ffi;

use std::fs::File;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::net::Shutdown;

pub use mapping::Mapping;
pub use protocol::{ControlMessage, ControlRange, Extent};
pub use uffd::UserfaultFd;

const HUGE_PAGE_SIZE: u64 = 2 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Protocol(String),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug)]
pub struct Client {
    socket: PathBuf,
}

impl Client {
    pub fn new(socket: impl AsRef<Path>) -> Self {
        Self {
            socket: socket.as_ref().to_path_buf(),
        }
    }

    pub fn open_memory_file(&self, path: impl Into<String>, size: u64) -> Result<Memory> {
        if size == 0 || size % HUGE_PAGE_SIZE != 0 {
            return Err(Error::Protocol(format!(
                "Smartmap size must be a non-zero {HUGE_PAGE_SIZE}-byte multiple"
            )));
        }
        let mut stream = UnixStream::connect(&self.socket)?;
        let req = protocol::Request::open(path.into(), size);
        let (resp, mut fds) =
            uds::send_json_recv_json_with_fds::<protocol::Response>(&mut stream, &req, &[])?;
        if fds.len() != 1 {
            return Err(Error::Protocol(format!(
                "open_memory_file returned {} fds, want 1",
                fds.len()
            )));
        }
        let fd = fds.remove(0);
        if !resp.ok {
            return Err(Error::Protocol(resp.error.unwrap_or_else(|| {
                "open_memory_file failed without error text".to_string()
            })));
        }
        let memory_id = resp
            .memory_id
            .ok_or_else(|| Error::Protocol("open_memory_file response missing memory_id".into()))?;
        Ok(Memory {
            client: self.clone(),
            memory_id,
            size,
            extents: resp.extents,
            memfd: unsafe { File::from_raw_fd(fd.into_raw_fd()) },
        })
    }
}

pub struct Memory {
    client: Client,
    memory_id: String,
    size: u64,
    extents: Vec<Extent>,
    memfd: File,
}

impl Memory {
    pub fn memory_id(&self) -> &str {
        &self.memory_id
    }

    pub fn extents(&self) -> &[Extent] {
        &self.extents
    }

    pub fn raw_fd(&self) -> i32 {
        self.memfd.as_raw_fd()
    }

    pub fn map_private_contiguous(&self) -> Result<Mapping> {
        Mapping::map_private_extents(self.memfd.as_raw_fd(), self.size, &self.extents)
    }

    pub fn create_userfaultfd(&self, mapping: &Mapping) -> Result<UserfaultFd> {
        UserfaultFd::new_registered(mapping.as_ptr() as usize, mapping.len())
    }

    pub unsafe fn attach_userfaultfd(&self, fd: OwnedFd) -> UserfaultFd {
        UserfaultFd::from_owned_fd(fd)
    }

    pub fn serve_faults(&self, uffd: &UserfaultFd, mapping: &Mapping) -> Result<FaultSession> {
        self.serve_faults_len(uffd, mapping, mapping.len())
    }

    pub fn serve_faults_len(
        &self,
        uffd: &UserfaultFd,
        mapping: &Mapping,
        len: usize,
    ) -> Result<FaultSession> {
        let mut stream = UnixStream::connect(&self.client.socket)?;
        let req =
            protocol::Request::serve_faults(self.memory_id.clone(), mapping.as_ptr() as usize, len);
        uds::send_json_with_fds(&mut stream, &req, &[uffd.as_raw_fd()])?;
        Ok(FaultSession {
            stream,
            memory_id: self.memory_id.clone(),
        })
    }

    pub fn close(self) -> Result<()> {
        self.close_ref()
    }

    pub fn close_ref(&self) -> Result<()> {
        let mut stream = UnixStream::connect(&self.client.socket)?;
        let req = protocol::Request::close(self.memory_id.clone());
        uds::send_json_with_fds(&mut stream, &req, &[])?;
        let resp = protocol::read_response(stream)?;
        if resp.ok {
            Ok(())
        } else {
            Err(Error::Protocol(resp.error.unwrap_or_else(|| {
                "close_memory_file failed without error text".to_string()
            })))
        }
    }
}

pub trait ControlHandler {
    fn evict(&mut self, ranges: &[ControlRange]) -> io::Result<()>;

    fn probe(&mut self, ranges: &[ControlRange]) -> io::Result<()> {
        self.evict(ranges)
    }
}

pub struct FaultSession {
    stream: UnixStream,
    memory_id: String,
}

impl FaultSession {
    pub fn shutdown(&self) -> io::Result<()> {
        self.stream.shutdown(Shutdown::Both)
    }

    pub fn handle_next_control<H: ControlHandler>(&mut self, handler: &mut H) -> Result<bool> {
        let msg = match protocol::read_control_message(&mut self.stream) {
            Ok(msg) => msg,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(false),
            Err(e) => return Err(e.into()),
        };
        let result = match msg.kind.as_str() {
            protocol::CONTROL_EVICT => handler.evict(&msg.ranges),
            protocol::CONTROL_PROBE => handler.probe(&msg.ranges),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported Smartmap control message {other}"),
            )),
        };
        protocol::write_control_ack(&mut self.stream, msg.ack(result.is_ok(), result.err()))?;
        Ok(true)
    }

    pub fn run_control_loop<H: ControlHandler>(&mut self, handler: &mut H) -> Result<()> {
        while self.handle_next_control(handler)? {}
        Ok(())
    }

    pub fn memory_id(&self) -> &str {
        &self.memory_id
    }
}
