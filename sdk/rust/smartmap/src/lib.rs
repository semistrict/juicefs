#![cfg(target_os = "linux")]

mod mapping;
mod protocol;
mod uds;
mod uffd;

pub mod ffi;

use std::fs::File;
use std::io;
use std::net::Shutdown;
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

use mapping::Mapping;
pub use mapping::{MutatorHooks, NoopMutatorHooks};
pub use protocol::{ControlRange, Extent};
use uffd::UserfaultFd;

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

    pub fn open_mapping(&self, path: impl Into<String>, size: u64) -> Result<SmartMapping> {
        if size == 0 || size % HUGE_PAGE_SIZE != 0 {
            return Err(Error::Protocol(format!(
                "Smartmap size must be a non-zero {HUGE_PAGE_SIZE}-byte multiple"
            )));
        }
        let stream = UnixStream::connect(&self.socket)?;
        let req = protocol::Frame::map(path.into(), size);
        uds::send_json_with_fds(&stream, &req, &[])?;

        let (mapped, mut fds) = uds::recv_json_with_fds::<protocol::Frame>(&stream)?;
        if mapped.kind == protocol::TYPE_FATAL {
            return Err(Error::Protocol(mapped.error));
        }
        if mapped.kind != protocol::TYPE_MAPPED {
            return Err(Error::Protocol(format!(
                "Smartmap expected mapped frame, got {}",
                mapped.kind
            )));
        }
        if mapped.fd != protocol::FD_MEMORY || fds.len() != 1 {
            return Err(Error::Protocol(format!(
                "mapped frame returned fd purpose {:?} with {} fds, want memory with 1 fd",
                mapped.fd,
                fds.len()
            )));
        }
        if mapped.page_size != HUGE_PAGE_SIZE as usize {
            return Err(Error::Protocol(format!(
                "mapped page_size got {}, want {}",
                mapped.page_size, HUGE_PAGE_SIZE
            )));
        }
        if mapped.size != size {
            return Err(Error::Protocol(format!(
                "mapped size got {}, want {}",
                mapped.size, size
            )));
        }

        let memfd = unsafe { File::from_raw_fd(fds.remove(0).into_raw_fd()) };
        let mapping =
            Mapping::map_private_extents(memfd.as_raw_fd(), mapped.size, &mapped.extents)?;
        let uffd = UserfaultFd::new_registered(mapping.as_ptr() as usize, mapping.len())?;
        let attach = protocol::Frame::attach(mapping.as_ptr() as usize, mapping.len());
        uds::send_json_with_fds(&stream, &attach, &[uffd.as_raw_fd()])?;
        let (attached, fds) = uds::recv_json_with_fds::<protocol::Frame>(&stream)?;
        if !fds.is_empty() {
            return Err(Error::Protocol(format!(
                "attached frame returned {} unexpected fds",
                fds.len()
            )));
        }
        if attached.kind == protocol::TYPE_FATAL {
            return Err(Error::Protocol(attached.error));
        }
        if attached.kind != protocol::TYPE_ATTACHED {
            return Err(Error::Protocol(format!(
                "Smartmap expected attached frame, got {}",
                attached.kind
            )));
        }

        Ok(SmartMapping {
            stream,
            memfd,
            extents: mapped.extents,
            mapping,
            uffd,
        })
    }
}

pub trait ControlHandler {
    fn release(&mut self, _ranges: &[ControlRange]) -> io::Result<()> {
        Ok(())
    }

    fn probe(&mut self, _ranges: &[ControlRange]) -> io::Result<()> {
        Ok(())
    }

    fn write_fault(&mut self, _ranges: &[ControlRange]) -> io::Result<()> {
        Ok(())
    }
}

pub struct SmartMapping {
    stream: UnixStream,
    memfd: File,
    extents: Vec<Extent>,
    mapping: Mapping,
    uffd: UserfaultFd,
}

impl SmartMapping {
    pub fn shutdown(&self) -> io::Result<()> {
        self.stream.shutdown(Shutdown::Both)
    }

    pub fn as_ptr(&self) -> *mut u8 {
        self.mapping.as_ptr()
    }

    pub fn len(&self) -> usize {
        self.mapping.len()
    }

    pub fn raw_fd(&self) -> i32 {
        self.memfd.as_raw_fd()
    }

    pub fn extents(&self) -> &[Extent] {
        &self.extents
    }

    pub fn sync_dirty<P: AsRef<Path>, H: MutatorHooks>(
        &self,
        writeback_path: P,
        hooks: &mut H,
    ) -> Result<()> {
        self.mapping.sync_dirty(&self.uffd, writeback_path, hooks)
    }

    pub fn handle_next_control<H: ControlHandler>(&self, handler: &mut H) -> Result<bool> {
        let (msg, fds) = match uds::recv_json_with_fds::<protocol::Frame>(&self.stream) {
            Ok(msg) => msg,
            Err(Error::Io(e)) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(false),
            Err(e) => return Err(e),
        };
        if !fds.is_empty() {
            return Err(Error::Protocol(format!(
                "control frame returned {} unexpected fds",
                fds.len()
            )));
        }
        if msg.kind == protocol::TYPE_FATAL {
            return Err(Error::Protocol(msg.error));
        }
        if msg.kind.is_empty() {
            return Ok(false);
        }

        let mut released = Vec::new();
        let result = match msg.kind.as_str() {
            protocol::TYPE_RELEASE => match self.mapping.release_clean_ranges(&msg.ranges) {
                Ok(ranges) => {
                    released = ranges;
                    handler.release(&released)
                }
                Err(e) => Err(io::Error::new(io::ErrorKind::Other, e.to_string())),
            },
            protocol::TYPE_PROBE => match self.mapping.release_clean_ranges(&msg.ranges) {
                Ok(ranges) => {
                    released = ranges;
                    handler.probe(&released)
                }
                Err(e) => Err(io::Error::new(io::ErrorKind::Other, e.to_string())),
            },
            protocol::TYPE_WRITE_FAULT => handler.write_fault(&msg.ranges),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported Smartmap control message {other}"),
            )),
        };
        let (ok, err) = match result {
            Ok(()) => (true, None),
            Err(e) => (false, Some(e.to_string())),
        };
        let ack = protocol::Frame::ack(ok, err, released);
        uds::send_json_with_fds(&self.stream, &ack, &[])?;
        Ok(true)
    }

    pub fn run_control_loop<H: ControlHandler>(&self, handler: &mut H) -> Result<()> {
        while self.handle_next_control(handler)? {}
        Ok(())
    }
}
