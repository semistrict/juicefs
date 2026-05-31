use std::io;
use std::os::fd::RawFd;
use std::os::unix::fs::FileExt;
use std::ptr;
use std::{
    fs::{File, OpenOptions},
    path::Path,
};

use crate::protocol::{ControlRange, Extent};
use crate::uffd::UserfaultFd;
use crate::{Error, Result, HUGE_PAGE_SIZE};

const PAGEMAP_ENTRY_SIZE: u64 = 8;
const PAGEMAP_PRESENT: u64 = 1 << 63;
const PAGEMAP_UFFD_WP: u64 = 1 << 57;

pub struct Mapping {
    ptr: *mut u8,
    len: usize,
    pagemap: File,
    page_size: u64,
}

impl Mapping {
    pub fn map_private_extents(fd: RawFd, size: u64, extents: &[Extent]) -> Result<Self> {
        if size == 0 || size % HUGE_PAGE_SIZE != 0 {
            return Err(Error::Protocol(format!(
                "mapping size must be a non-zero {HUGE_PAGE_SIZE}-byte multiple"
            )));
        }
        let len = usize::try_from(size).map_err(|_| Error::Protocol("mapping too large".into()))?;
        let reserve = unsafe {
            libc::mmap(
                ptr::null_mut(),
                len,
                libc::PROT_NONE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if reserve == libc::MAP_FAILED {
            return Err(io::Error::last_os_error().into());
        }
        for extent in extents {
            let addr = unsafe { (reserve as *mut u8).add(extent.file_offset as usize) };
            let mapped = unsafe {
                libc::mmap(
                    addr as *mut libc::c_void,
                    extent.length as usize,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_PRIVATE | libc::MAP_FIXED,
                    fd,
                    extent.shm_offset as libc::off_t,
                )
            };
            if mapped == libc::MAP_FAILED {
                let err = io::Error::last_os_error();
                unsafe {
                    libc::munmap(reserve, len);
                }
                return Err(err.into());
            }
        }
        let pagemap = match OpenOptions::new().read(true).open("/proc/self/pagemap") {
            Ok(pagemap) => pagemap,
            Err(err) => {
                unsafe {
                    libc::munmap(reserve, len);
                }
                return Err(err.into());
            }
        };
        let page_size = match host_page_size() {
            Ok(page_size) => page_size,
            Err(err) => {
                unsafe {
                    libc::munmap(reserve, len);
                }
                return Err(err.into());
            }
        };
        Ok(Self {
            ptr: reserve as *mut u8,
            len,
            pagemap,
            page_size,
        })
    }

    pub fn as_ptr(&self) -> *mut u8 {
        self.ptr
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub unsafe fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    pub fn release_clean_ranges(&self, ranges: &[ControlRange]) -> Result<Vec<ControlRange>> {
        let mut released = Vec::new();
        for range in ranges {
            let (off, len) = self.control_range(range)?;
            if !self.page_state(off)?.releasable() {
                continue;
            }
            let rc = unsafe {
                libc::madvise(
                    self.ptr.add(off) as *mut libc::c_void,
                    len,
                    libc::MADV_DONTNEED,
                )
            };
            if rc != 0 {
                return Err(io::Error::last_os_error().into());
            }
            released.push(range.clone());
        }
        Ok(released)
    }

    pub fn sync_dirty<P: AsRef<Path>, H: MutatorHooks>(
        &self,
        uffd: &UserfaultFd,
        writeback_path: P,
        hooks: &mut H,
    ) -> Result<()> {
        if self.len as u64 % HUGE_PAGE_SIZE != 0 {
            return Err(Error::Protocol(
                "mapping length is not 2 MiB aligned".into(),
            ));
        }
        hooks.pause()?;
        let protect_result: Result<Vec<usize>> = (|| {
            let dirty = self.dirty_pages()?;
            for off in &dirty {
                uffd.write_protect(self.ptr as usize + *off, HUGE_PAGE_SIZE as usize, true)?;
            }
            Ok(dirty)
        })();
        let resume = hooks.resume();
        let dirty = protect_result?;
        resume?;
        self.flush_pages(uffd, writeback_path, &dirty, hooks)?;
        Ok(())
    }

    fn dirty_pages(&self) -> Result<Vec<usize>> {
        let mut dirty = Vec::new();
        for off in (0..self.len).step_by(HUGE_PAGE_SIZE as usize) {
            if !self.page_state(off)?.dirty() {
                continue;
            }
            dirty.push(off);
        }
        Ok(dirty)
    }

    fn control_range(&self, range: &ControlRange) -> Result<(usize, usize)> {
        if range.file_offset % HUGE_PAGE_SIZE != 0 || range.length != HUGE_PAGE_SIZE {
            return Err(Error::Protocol(format!(
                "control range [{}, {}) is not one 2 MiB page",
                range.file_offset,
                range.file_offset + range.length
            )));
        }
        let off = usize::try_from(range.file_offset)
            .map_err(|_| Error::Protocol("control range offset too large".into()))?;
        let len = usize::try_from(range.length)
            .map_err(|_| Error::Protocol("control range length too large".into()))?;
        let Some(end) = off.checked_add(len) else {
            return Err(Error::Protocol(format!(
                "control range [{}, {}) overflows mapping length {}",
                off,
                off.saturating_add(len),
                self.len
            )));
        };
        if end > self.len {
            return Err(Error::Protocol(format!(
                "control range [{}, {}) exceeds mapping length {}",
                off, end, self.len
            )));
        }
        Ok((off, len))
    }

    fn page_state(&self, off: usize) -> Result<PageState> {
        let entry = pagemap_entry(&self.pagemap, self.ptr as usize + off, self.page_size)?;
        if entry & PAGEMAP_PRESENT == 0 {
            return Ok(PageState::Absent);
        }
        if entry & PAGEMAP_UFFD_WP != 0 {
            return Ok(PageState::WriteProtected);
        }
        Ok(PageState::Dirty)
    }

    fn flush_pages<P: AsRef<Path>, H: MutatorHooks>(
        &self,
        uffd: &UserfaultFd,
        writeback_path: P,
        pages: &[usize],
        hooks: &mut H,
    ) -> Result<()> {
        if pages.is_empty() {
            return Ok(());
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(writeback_path)?;
        let data = unsafe { self.as_slice() };
        for off in pages {
            let end = *off + HUGE_PAGE_SIZE as usize;
            file.write_all_at(&data[*off..end], *off as u64)?;
            uffd.write_protect(self.ptr as usize + *off, HUGE_PAGE_SIZE as usize, false)?;
            hooks.page_synced(*off)?;
        }
        file.sync_all()?;
        Ok(())
    }
}

enum PageState {
    Absent,
    WriteProtected,
    Dirty,
}

impl PageState {
    fn dirty(&self) -> bool {
        matches!(self, Self::Dirty)
    }

    fn releasable(&self) -> bool {
        matches!(self, Self::Absent | Self::WriteProtected)
    }
}

pub trait MutatorHooks {
    fn pause(&mut self) -> Result<()>;
    fn resume(&mut self) -> Result<()>;
    fn page_synced(&mut self, _offset: usize) -> Result<()> {
        Ok(())
    }
}

pub struct NoopMutatorHooks;

impl MutatorHooks for NoopMutatorHooks {
    fn pause(&mut self) -> Result<()> {
        Ok(())
    }

    fn resume(&mut self) -> Result<()> {
        Ok(())
    }
}

fn host_page_size() -> io::Result<u64> {
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page_size <= 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(page_size as u64)
}

fn pagemap_entry(pagemap: &std::fs::File, addr: usize, page_size: u64) -> io::Result<u64> {
    let vpn = addr as u64 / page_size;
    let mut buf = [0u8; PAGEMAP_ENTRY_SIZE as usize];
    pagemap.read_exact_at(&mut buf, vpn * PAGEMAP_ENTRY_SIZE)?;
    Ok(u64::from_le_bytes(buf))
}

impl Drop for Mapping {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.ptr as *mut libc::c_void, self.len);
        }
    }
}

unsafe impl Send for Mapping {}
