#![allow(non_camel_case_types)]

use std::ffi::{CStr, CString};
use std::io;
use std::os::fd::{FromRawFd, OwnedFd};
use std::os::raw::{c_char, c_int, c_void};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;

use crate::{
    Client, ControlHandler, ControlRange, FaultSession, Mapping, Memory, Result, UserfaultFd,
};

#[repr(C)]
#[derive(Clone, Copy)]
pub struct jfs_smartmap_range {
    pub file_offset: u64,
    pub length: u64,
    pub shm_offset: u64,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct jfs_smartmap_extent {
    pub file_offset: u64,
    pub length: u64,
    pub shm_offset: u64,
}

#[repr(C)]
pub struct jfs_smartmap_callbacks {
    pub userdata: *mut c_void,
    pub evict: Option<
        unsafe extern "C" fn(
            userdata: *mut c_void,
            ranges: *const jfs_smartmap_range,
            len: usize,
        ) -> c_int,
    >,
    pub probe: Option<
        unsafe extern "C" fn(
            userdata: *mut c_void,
            ranges: *const jfs_smartmap_range,
            len: usize,
        ) -> c_int,
    >,
}

pub struct jfs_smartmap_client {
    inner: Client,
}

pub struct jfs_smartmap_memory {
    inner: Option<Memory>,
}

pub struct jfs_smartmap_mapping {
    inner: Mapping,
}

pub struct jfs_smartmap_uffd {
    inner: UserfaultFd,
}

pub struct jfs_smartmap_session {
    inner: FaultSession,
}

#[no_mangle]
pub extern "C" fn jfs_smartmap_client_new(
    socket: *const c_char,
    out: *mut *mut jfs_smartmap_client,
    err: *mut *mut c_char,
) -> c_int {
    ffi_result(err, || {
        let socket = cstr(socket)?;
        write_out(
            out,
            Box::into_raw(Box::new(jfs_smartmap_client {
                inner: Client::new(socket),
            })),
        )
    })
}

#[no_mangle]
pub extern "C" fn jfs_smartmap_client_free(client: *mut jfs_smartmap_client) {
    free_box(client);
}

#[no_mangle]
pub extern "C" fn jfs_smartmap_open_memory(
    client: *const jfs_smartmap_client,
    path: *const c_char,
    size: u64,
    out: *mut *mut jfs_smartmap_memory,
    err: *mut *mut c_char,
) -> c_int {
    ffi_result(err, || {
        let client = unsafe_ref(client, "client")?;
        let path = cstr(path)?;
        let memory = client.inner.open_memory_file(path, size)?;
        write_out(
            out,
            Box::into_raw(Box::new(jfs_smartmap_memory {
                inner: Some(memory),
            })),
        )
    })
}

#[no_mangle]
pub extern "C" fn jfs_smartmap_memory_id(memory: *const jfs_smartmap_memory) -> *const c_char {
    let Ok(memory) = unsafe_ref(memory, "memory") else {
        return ptr::null();
    };
    let Some(memory) = memory.inner.as_ref() else {
        return ptr::null();
    };
    match CString::new(memory.memory_id()) {
        Ok(s) => s.into_raw(),
        Err(_) => ptr::null(),
    }
}

#[no_mangle]
pub extern "C" fn jfs_smartmap_memory_raw_fd(memory: *const jfs_smartmap_memory) -> c_int {
    let Ok(memory) = unsafe_ref(memory, "memory") else {
        return -1;
    };
    let Some(memory) = memory.inner.as_ref() else {
        return -1;
    };
    memory.raw_fd()
}

#[no_mangle]
pub extern "C" fn jfs_smartmap_memory_extent_count(memory: *const jfs_smartmap_memory) -> usize {
    let Ok(memory) = unsafe_ref(memory, "memory") else {
        return 0;
    };
    let Some(memory) = memory.inner.as_ref() else {
        return 0;
    };
    memory.extents().len()
}

#[no_mangle]
pub extern "C" fn jfs_smartmap_memory_extent_at(
    memory: *const jfs_smartmap_memory,
    index: usize,
    out: *mut jfs_smartmap_extent,
) -> c_int {
    let Ok(memory) = unsafe_ref(memory, "memory") else {
        return -1;
    };
    let Some(memory) = memory.inner.as_ref() else {
        return -1;
    };
    let Some(extent) = memory.extents().get(index) else {
        return -1;
    };
    if out.is_null() {
        return -1;
    }
    unsafe {
        *out = jfs_smartmap_extent {
            file_offset: extent.file_offset,
            length: extent.length,
            shm_offset: extent.shm_offset,
        };
    }
    0
}

#[no_mangle]
pub extern "C" fn jfs_smartmap_memory_close(
    memory: *mut jfs_smartmap_memory,
    err: *mut *mut c_char,
) -> c_int {
    ffi_result(err, || {
        let memory = unsafe_mut(memory, "memory")?;
        let inner = memory
            .inner
            .as_ref()
            .ok_or_else(|| crate::Error::Protocol("memory already closed".into()))?;
        inner.close_ref()?;
        memory.inner.take();
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn jfs_smartmap_memory_free(memory: *mut jfs_smartmap_memory) {
    free_box(memory);
}

#[no_mangle]
pub extern "C" fn jfs_smartmap_map_private(
    memory: *const jfs_smartmap_memory,
    out: *mut *mut jfs_smartmap_mapping,
    err: *mut *mut c_char,
) -> c_int {
    ffi_result(err, || {
        let memory = unsafe_ref(memory, "memory")?;
        let memory = memory
            .inner
            .as_ref()
            .ok_or_else(|| crate::Error::Protocol("memory is closed".into()))?;
        let mapping = memory.map_private_contiguous()?;
        write_out(
            out,
            Box::into_raw(Box::new(jfs_smartmap_mapping { inner: mapping })),
        )
    })
}

#[no_mangle]
pub extern "C" fn jfs_smartmap_mapping_ptr(mapping: *const jfs_smartmap_mapping) -> *mut c_void {
    let Ok(mapping) = unsafe_ref(mapping, "mapping") else {
        return ptr::null_mut();
    };
    mapping.inner.as_ptr() as *mut c_void
}

#[no_mangle]
pub extern "C" fn jfs_smartmap_mapping_len(mapping: *const jfs_smartmap_mapping) -> usize {
    let Ok(mapping) = unsafe_ref(mapping, "mapping") else {
        return 0;
    };
    mapping.inner.len()
}

#[no_mangle]
pub extern "C" fn jfs_smartmap_mapping_free(mapping: *mut jfs_smartmap_mapping) {
    free_box(mapping);
}

#[no_mangle]
pub extern "C" fn jfs_smartmap_create_uffd(
    memory: *const jfs_smartmap_memory,
    mapping: *const jfs_smartmap_mapping,
    out: *mut *mut jfs_smartmap_uffd,
    err: *mut *mut c_char,
) -> c_int {
    ffi_result(err, || {
        let memory = unsafe_ref(memory, "memory")?;
        let memory = memory
            .inner
            .as_ref()
            .ok_or_else(|| crate::Error::Protocol("memory is closed".into()))?;
        let mapping = unsafe_ref(mapping, "mapping")?;
        let uffd = memory.create_userfaultfd(&mapping.inner)?;
        write_out(
            out,
            Box::into_raw(Box::new(jfs_smartmap_uffd { inner: uffd })),
        )
    })
}

#[no_mangle]
pub extern "C" fn jfs_smartmap_uffd_from_fd(
    fd: c_int,
    out: *mut *mut jfs_smartmap_uffd,
    err: *mut *mut c_char,
) -> c_int {
    ffi_result(err, || {
        if fd < 0 {
            return Err(crate::Error::Protocol(format!("invalid uffd fd {fd}")));
        }
        let uffd = unsafe { UserfaultFd::from_owned_fd(OwnedFd::from_raw_fd(fd)) };
        write_out(
            out,
            Box::into_raw(Box::new(jfs_smartmap_uffd { inner: uffd })),
        )
    })
}

#[no_mangle]
pub extern "C" fn jfs_smartmap_uffd_raw_fd(uffd: *const jfs_smartmap_uffd) -> c_int {
    let Ok(uffd) = unsafe_ref(uffd, "uffd") else {
        return -1;
    };
    std::os::fd::AsRawFd::as_raw_fd(&uffd.inner)
}

#[no_mangle]
pub extern "C" fn jfs_smartmap_uffd_free(uffd: *mut jfs_smartmap_uffd) {
    free_box(uffd);
}

#[no_mangle]
pub extern "C" fn jfs_smartmap_serve_faults(
    memory: *const jfs_smartmap_memory,
    uffd: *const jfs_smartmap_uffd,
    mapping: *const jfs_smartmap_mapping,
    out: *mut *mut jfs_smartmap_session,
    err: *mut *mut c_char,
) -> c_int {
    ffi_result(err, || {
        let memory = unsafe_ref(memory, "memory")?;
        let memory = memory
            .inner
            .as_ref()
            .ok_or_else(|| crate::Error::Protocol("memory is closed".into()))?;
        let uffd = unsafe_ref(uffd, "uffd")?;
        let mapping = unsafe_ref(mapping, "mapping")?;
        let session = memory.serve_faults(&uffd.inner, &mapping.inner)?;
        write_out(
            out,
            Box::into_raw(Box::new(jfs_smartmap_session { inner: session })),
        )
    })
}

#[no_mangle]
pub extern "C" fn jfs_smartmap_serve_faults_len(
    memory: *const jfs_smartmap_memory,
    uffd: *const jfs_smartmap_uffd,
    mapping: *const jfs_smartmap_mapping,
    len: usize,
    out: *mut *mut jfs_smartmap_session,
    err: *mut *mut c_char,
) -> c_int {
    ffi_result(err, || {
        let memory = unsafe_ref(memory, "memory")?;
        let memory = memory
            .inner
            .as_ref()
            .ok_or_else(|| crate::Error::Protocol("memory is closed".into()))?;
        let uffd = unsafe_ref(uffd, "uffd")?;
        let mapping = unsafe_ref(mapping, "mapping")?;
        let session = memory.serve_faults_len(&uffd.inner, &mapping.inner, len)?;
        write_out(
            out,
            Box::into_raw(Box::new(jfs_smartmap_session { inner: session })),
        )
    })
}

#[no_mangle]
pub extern "C" fn jfs_smartmap_run_control_loop(
    session: *mut jfs_smartmap_session,
    callbacks: *const jfs_smartmap_callbacks,
    err: *mut *mut c_char,
) -> c_int {
    ffi_result(err, || {
        let session = unsafe_mut(session, "session")?;
        let callbacks = unsafe_ref(callbacks, "callbacks")?;
        let mut handler = FfiHandler { callbacks };
        session.inner.run_control_loop(&mut handler)
    })
}

#[no_mangle]
pub extern "C" fn jfs_smartmap_handle_next_control(
    session: *mut jfs_smartmap_session,
    callbacks: *const jfs_smartmap_callbacks,
    handled: *mut c_int,
    err: *mut *mut c_char,
) -> c_int {
    ffi_result(err, || {
        let session = unsafe_mut(session, "session")?;
        let callbacks = unsafe_ref(callbacks, "callbacks")?;
        if handled.is_null() {
            return Err(crate::Error::Protocol("null handled output pointer".into()));
        }
        let mut handler = FfiHandler { callbacks };
        let did_handle = session.inner.handle_next_control(&mut handler)?;
        unsafe {
            *handled = if did_handle { 1 } else { 0 };
        }
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn jfs_smartmap_session_shutdown(
    session: *mut jfs_smartmap_session,
    err: *mut *mut c_char,
) -> c_int {
    ffi_result(err, || {
        let session = unsafe_ref(session, "session")?;
        session.inner.shutdown()?;
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn jfs_smartmap_session_free(session: *mut jfs_smartmap_session) {
    free_box(session);
}

#[no_mangle]
pub extern "C" fn jfs_smartmap_string_free(s: *mut c_char) {
    if !s.is_null() {
        unsafe {
            let _ = CString::from_raw(s);
        }
    }
}

struct FfiHandler<'a> {
    callbacks: &'a jfs_smartmap_callbacks,
}

impl ControlHandler for FfiHandler<'_> {
    fn evict(&mut self, ranges: &[ControlRange]) -> io::Result<()> {
        call_control(self.callbacks, self.callbacks.evict, ranges)
    }

    fn probe(&mut self, ranges: &[ControlRange]) -> io::Result<()> {
        let cb = self.callbacks.probe.or(self.callbacks.evict);
        call_control(self.callbacks, cb, ranges)
    }
}

fn call_control(
    callbacks: &jfs_smartmap_callbacks,
    cb: Option<unsafe extern "C" fn(*mut c_void, *const jfs_smartmap_range, usize) -> c_int>,
    ranges: &[ControlRange],
) -> io::Result<()> {
    let Some(cb) = cb else {
        return Ok(());
    };
    let c_ranges: Vec<jfs_smartmap_range> = ranges
        .iter()
        .map(|r| jfs_smartmap_range {
            file_offset: r.file_offset,
            length: r.length,
            shm_offset: r.shm_offset,
        })
        .collect();
    let rc = unsafe { cb(callbacks.userdata, c_ranges.as_ptr(), c_ranges.len()) };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::Other,
            format!("Smartmap control callback failed with {rc}"),
        ))
    }
}

fn ffi_result(err: *mut *mut c_char, f: impl FnOnce() -> Result<()>) -> c_int {
    clear_error(err);
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(())) => 0,
        Ok(Err(e)) => {
            set_error(err, e.to_string());
            -1
        }
        Err(_) => {
            set_error(err, "panic crossing Smartmap C API".into());
            -1
        }
    }
}

fn cstr(ptr: *const c_char) -> Result<String> {
    if ptr.is_null() {
        return Err(crate::Error::Protocol("null C string".into()));
    }
    unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .map(|s| s.to_string())
        .map_err(|e| crate::Error::Protocol(format!("invalid UTF-8 C string: {e}")))
}

fn unsafe_ref<'a, T>(ptr: *const T, name: &str) -> Result<&'a T> {
    if ptr.is_null() {
        return Err(crate::Error::Protocol(format!("null {name}")));
    }
    Ok(unsafe { &*ptr })
}

fn unsafe_mut<'a, T>(ptr: *mut T, name: &str) -> Result<&'a mut T> {
    if ptr.is_null() {
        return Err(crate::Error::Protocol(format!("null {name}")));
    }
    Ok(unsafe { &mut *ptr })
}

fn write_out<T>(out: *mut *mut T, value: *mut T) -> Result<()> {
    if out.is_null() {
        unsafe {
            let _ = Box::from_raw(value);
        }
        return Err(crate::Error::Protocol("null output pointer".into()));
    }
    unsafe {
        *out = value;
    }
    Ok(())
}

fn free_box<T>(ptr: *mut T) {
    if !ptr.is_null() {
        unsafe {
            let _ = Box::from_raw(ptr);
        }
    }
}

fn clear_error(err: *mut *mut c_char) {
    if !err.is_null() {
        unsafe {
            *err = ptr::null_mut();
        }
    }
}

fn set_error(err: *mut *mut c_char, msg: String) {
    if err.is_null() {
        return;
    }
    let safe = msg.replace('\0', "\\0");
    let Ok(cstr) = CString::new(safe) else {
        return;
    };
    unsafe {
        *err = cstr.into_raw();
    }
}
