use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

use crate::{Result, HUGE_PAGE_SIZE};

const UFFD_API: u64 = 0xAA;
const UFFD_FEATURE_MISSING_HUGETLBFS: u64 = 1 << 4;
const UFFD_FEATURE_MINOR_HUGETLBFS: u64 = 1 << 9;
const UFFDIO_REGISTER_MODE_MISSING: u64 = 1 << 0;
const UFFDIO_REGISTER_MODE_WP: u64 = 1 << 1;
const UFFDIO_REGISTER_MODE_MINOR: u64 = 1 << 2;
const UFFDIO_WRITEPROTECT_MODE_WP: u64 = 1 << 0;

const IOC_NRBITS: u64 = 8;
const IOC_TYPEBITS: u64 = 8;
const IOC_SIZEBITS: u64 = 14;
const IOC_NRSHIFT: u64 = 0;
const IOC_TYPESHIFT: u64 = IOC_NRSHIFT + IOC_NRBITS;
const IOC_SIZESHIFT: u64 = IOC_TYPESHIFT + IOC_TYPEBITS;
const IOC_DIRSHIFT: u64 = IOC_SIZESHIFT + IOC_SIZEBITS;
const IOC_WRITE: u64 = 1;
const IOC_READ: u64 = 2;

#[repr(C)]
struct UffdioApi {
    api: u64,
    features: u64,
    ioctls: u64,
}

#[repr(C)]
struct UffdioRange {
    start: u64,
    len: u64,
}

#[repr(C)]
struct UffdioRegister {
    range: UffdioRange,
    mode: u64,
    ioctls: u64,
}

#[repr(C)]
struct UffdioWriteProtect {
    range: UffdioRange,
    mode: u64,
}

pub struct UserfaultFd {
    fd: OwnedFd,
}

impl UserfaultFd {
    pub unsafe fn from_owned_fd(fd: OwnedFd) -> Self {
        Self { fd }
    }

    pub fn new_registered(base: usize, len: usize) -> Result<Self> {
        if len == 0 || len as u64 % HUGE_PAGE_SIZE != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "UFFD range must be 2 MiB aligned",
            )
            .into());
        }
        let raw =
            unsafe { libc::syscall(libc::SYS_userfaultfd, libc::O_CLOEXEC | libc::O_NONBLOCK) };
        if raw < 0 {
            return Err(io::Error::last_os_error().into());
        }
        let fd = unsafe { OwnedFd::from_raw_fd(raw as RawFd) };
        let mut api = UffdioApi {
            api: UFFD_API,
            features: UFFD_FEATURE_MISSING_HUGETLBFS | UFFD_FEATURE_MINOR_HUGETLBFS,
            ioctls: 0,
        };
        ioctl(fd.as_raw_fd(), uffdio_api_ioctl(), &mut api)?;
        let mut reg = UffdioRegister {
            range: UffdioRange {
                start: base as u64,
                len: len as u64,
            },
            mode: UFFDIO_REGISTER_MODE_MISSING
                | UFFDIO_REGISTER_MODE_MINOR
                | UFFDIO_REGISTER_MODE_WP,
            ioctls: 0,
        };
        ioctl(fd.as_raw_fd(), uffdio_register_ioctl(), &mut reg)?;
        Ok(Self { fd })
    }

    pub fn write_protect(&self, addr: usize, len: usize, protect: bool) -> Result<()> {
        let mut wp = UffdioWriteProtect {
            range: UffdioRange {
                start: addr as u64,
                len: len as u64,
            },
            mode: if protect {
                UFFDIO_WRITEPROTECT_MODE_WP
            } else {
                0
            },
        };
        ioctl(self.fd.as_raw_fd(), uffdio_writeprotect_ioctl(), &mut wp)?;
        Ok(())
    }
}

impl AsRawFd for UserfaultFd {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

fn ioctl<T>(fd: RawFd, req: u64, arg: &mut T) -> io::Result<()> {
    let rc = unsafe { libc::ioctl(fd, req as libc::c_ulong, arg as *mut T) };
    if rc < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

const fn iowr(nr: u64, size: u64) -> u64 {
    ((IOC_READ | IOC_WRITE) << IOC_DIRSHIFT)
        | (0xAA << IOC_TYPESHIFT)
        | (nr << IOC_NRSHIFT)
        | (size << IOC_SIZESHIFT)
}

const fn uffdio_api_ioctl() -> u64 {
    iowr(0x3F, std::mem::size_of::<UffdioApi>() as u64)
}

const fn uffdio_register_ioctl() -> u64 {
    iowr(0x00, std::mem::size_of::<UffdioRegister>() as u64)
}

const fn uffdio_writeprotect_ioctl() -> u64 {
    iowr(0x06, std::mem::size_of::<UffdioWriteProtect>() as u64)
}
