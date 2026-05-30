#![allow(non_camel_case_types)]

use std::ffi::{CStr, CString};
use std::io;
use std::os::raw::{c_char, c_int, c_void};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;

use crate::{Client, ControlHandler, ControlRange, MutatorHooks, Result, SmartMapping};

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
    pub release: Option<
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
    pub write_fault: Option<
        unsafe extern "C" fn(
            userdata: *mut c_void,
            ranges: *const jfs_smartmap_range,
            len: usize,
        ) -> c_int,
    >,
}

#[repr(C)]
pub struct jfs_smartmap_sync_callbacks {
    pub userdata: *mut c_void,
    pub pause: Option<unsafe extern "C" fn(userdata: *mut c_void) -> c_int>,
    pub resume: Option<unsafe extern "C" fn(userdata: *mut c_void) -> c_int>,
    pub page_synced: Option<unsafe extern "C" fn(userdata: *mut c_void, offset: usize) -> c_int>,
}

pub struct jfs_smartmap_client {
    inner: Client,
}

pub struct jfs_smartmap_mapping {
    inner: SmartMapping,
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
pub extern "C" fn jfs_smartmap_mapping_open(
    client: *const jfs_smartmap_client,
    path: *const c_char,
    size: u64,
    out: *mut *mut jfs_smartmap_mapping,
    err: *mut *mut c_char,
) -> c_int {
    ffi_result(err, || {
        let client = unsafe_ref(client, "client")?;
        let path = cstr(path)?;
        let mapping = client.inner.open_mapping(path, size)?;
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
pub extern "C" fn jfs_smartmap_mapping_raw_fd(mapping: *const jfs_smartmap_mapping) -> c_int {
    let Ok(mapping) = unsafe_ref(mapping, "mapping") else {
        return -1;
    };
    mapping.inner.raw_fd()
}

#[no_mangle]
pub extern "C" fn jfs_smartmap_mapping_extent_count(mapping: *const jfs_smartmap_mapping) -> usize {
    let Ok(mapping) = unsafe_ref(mapping, "mapping") else {
        return 0;
    };
    mapping.inner.extents().len()
}

#[no_mangle]
pub extern "C" fn jfs_smartmap_mapping_extent_at(
    mapping: *const jfs_smartmap_mapping,
    index: usize,
    out: *mut jfs_smartmap_extent,
) -> c_int {
    let Ok(mapping) = unsafe_ref(mapping, "mapping") else {
        return -1;
    };
    let Some(extent) = mapping.inner.extents().get(index) else {
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
pub extern "C" fn jfs_smartmap_mapping_sync(
    mapping: *const jfs_smartmap_mapping,
    writeback_path: *const c_char,
    callbacks: *const jfs_smartmap_sync_callbacks,
    err: *mut *mut c_char,
) -> c_int {
    ffi_result(err, || {
        let mapping = unsafe_ref(mapping, "mapping")?;
        let writeback_path = cstr(writeback_path)?;
        let callbacks = unsafe_ref(callbacks, "callbacks")?;
        let mut hooks = FfiMutatorHooks { callbacks };
        mapping.inner.sync_dirty(writeback_path, &mut hooks)
    })
}

#[no_mangle]
pub extern "C" fn jfs_smartmap_run_control_loop(
    mapping: *mut jfs_smartmap_mapping,
    callbacks: *const jfs_smartmap_callbacks,
    err: *mut *mut c_char,
) -> c_int {
    ffi_result(err, || {
        let mapping = unsafe_ref(mapping, "mapping")?;
        let callbacks = unsafe_ref(callbacks, "callbacks")?;
        let mut handler = FfiHandler { callbacks };
        mapping.inner.run_control_loop(&mut handler)
    })
}

#[no_mangle]
pub extern "C" fn jfs_smartmap_handle_next_control(
    mapping: *mut jfs_smartmap_mapping,
    callbacks: *const jfs_smartmap_callbacks,
    handled: *mut c_int,
    err: *mut *mut c_char,
) -> c_int {
    ffi_result(err, || {
        let mapping = unsafe_ref(mapping, "mapping")?;
        let callbacks = unsafe_ref(callbacks, "callbacks")?;
        if handled.is_null() {
            return Err(crate::Error::Protocol("null handled output pointer".into()));
        }
        let mut handler = FfiHandler { callbacks };
        let did_handle = mapping.inner.handle_next_control(&mut handler)?;
        unsafe {
            *handled = if did_handle { 1 } else { 0 };
        }
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn jfs_smartmap_mapping_shutdown(
    mapping: *mut jfs_smartmap_mapping,
    err: *mut *mut c_char,
) -> c_int {
    ffi_result(err, || {
        let mapping = unsafe_ref(mapping, "mapping")?;
        mapping.inner.shutdown()?;
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn jfs_smartmap_mapping_free(mapping: *mut jfs_smartmap_mapping) {
    free_box(mapping);
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

struct FfiMutatorHooks<'a> {
    callbacks: &'a jfs_smartmap_sync_callbacks,
}

impl ControlHandler for FfiHandler<'_> {
    fn release(&mut self, ranges: &[ControlRange]) -> io::Result<()> {
        call_control(self.callbacks, self.callbacks.release, ranges)
    }

    fn probe(&mut self, ranges: &[ControlRange]) -> io::Result<()> {
        call_control(self.callbacks, self.callbacks.probe, ranges)
    }

    fn write_fault(&mut self, ranges: &[ControlRange]) -> io::Result<()> {
        call_control(self.callbacks, self.callbacks.write_fault, ranges)
    }
}

impl MutatorHooks for FfiMutatorHooks<'_> {
    fn pause(&mut self) -> Result<()> {
        call_mutator(self.callbacks, self.callbacks.pause, "pause")
    }

    fn resume(&mut self) -> Result<()> {
        call_mutator(self.callbacks, self.callbacks.resume, "resume")
    }

    fn page_synced(&mut self, offset: usize) -> Result<()> {
        call_page_synced(self.callbacks, offset)
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

fn call_mutator(
    callbacks: &jfs_smartmap_sync_callbacks,
    cb: Option<unsafe extern "C" fn(*mut c_void) -> c_int>,
    name: &str,
) -> Result<()> {
    let Some(cb) = cb else {
        return Ok(());
    };
    let rc = unsafe { cb(callbacks.userdata) };
    if rc == 0 {
        Ok(())
    } else {
        Err(crate::Error::Protocol(format!(
            "Smartmap mutator {name} callback failed with {rc}"
        )))
    }
}

fn call_page_synced(callbacks: &jfs_smartmap_sync_callbacks, offset: usize) -> Result<()> {
    let Some(cb) = callbacks.page_synced else {
        return Ok(());
    };
    let rc = unsafe { cb(callbacks.userdata, offset) };
    if rc == 0 {
        Ok(())
    } else {
        Err(crate::Error::Protocol(format!(
            "Smartmap page_synced callback failed with {rc}"
        )))
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
