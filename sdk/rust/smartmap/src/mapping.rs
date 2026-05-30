use std::io;
use std::os::fd::RawFd;
use std::ptr;

use crate::protocol::Extent;
use crate::{Error, Result, HUGE_PAGE_SIZE};

pub struct Mapping {
    ptr: *mut u8,
    len: usize,
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
        Ok(Self {
            ptr: reserve as *mut u8,
            len,
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

    pub unsafe fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

impl Drop for Mapping {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.ptr as *mut libc::c_void, self.len);
        }
    }
}

unsafe impl Send for Mapping {}
