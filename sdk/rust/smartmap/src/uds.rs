use std::io;
use std::mem;
use std::os::fd::{FromRawFd, OwnedFd, RawFd};
use std::os::unix::net::UnixStream;

use serde::{de::DeserializeOwned, Serialize};

use crate::Result;

const PAYLOAD_SIZE: usize = 64 * 1024;

pub fn send_json_with_fds<T: Serialize>(
    stream: &mut UnixStream,
    value: &T,
    fds: &[RawFd],
) -> Result<()> {
    let payload = serde_json::to_vec(value)?;
    sendmsg(stream, &payload, fds)?;
    Ok(())
}

pub fn send_json_recv_json_with_fds<T: DeserializeOwned>(
    stream: &mut UnixStream,
    value: &impl Serialize,
    fds: &[RawFd],
) -> Result<(T, Vec<OwnedFd>)> {
    let payload = serde_json::to_vec(value)?;
    sendmsg(stream, &payload, fds)?;
    recv_fds::<T>(stream)
}

fn sendmsg(stream: &UnixStream, payload: &[u8], fds: &[RawFd]) -> io::Result<()> {
    let iov = libc::iovec {
        iov_base: payload.as_ptr() as *mut libc::c_void,
        iov_len: payload.len(),
    };
    let mut control = vec![0u8; cmsg_space(mem::size_of_val(fds))];
    let mut msg: libc::msghdr = unsafe { mem::zeroed() };
    msg.msg_iov = &iov as *const _ as *mut _;
    msg.msg_iovlen = 1;
    if !fds.is_empty() {
        msg.msg_control = control.as_mut_ptr() as *mut libc::c_void;
        msg.msg_controllen = control.len();
        unsafe {
            let cmsg = libc::CMSG_FIRSTHDR(&msg);
            (*cmsg).cmsg_level = libc::SOL_SOCKET;
            (*cmsg).cmsg_type = libc::SCM_RIGHTS;
            (*cmsg).cmsg_len = cmsg_len(mem::size_of_val(fds));
            std::ptr::copy_nonoverlapping(
                fds.as_ptr() as *const u8,
                libc::CMSG_DATA(cmsg),
                mem::size_of_val(fds),
            );
            msg.msg_controllen = (*cmsg).cmsg_len;
        }
    }
    let n = unsafe { libc::sendmsg(stream.as_raw_fd(), &msg, 0) };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn recv_fds<T: DeserializeOwned>(stream: &UnixStream) -> Result<(T, Vec<OwnedFd>)> {
    let mut payload = vec![0u8; PAYLOAD_SIZE];
    let mut control = vec![0u8; cmsg_space(mem::size_of::<RawFd>() * 8)];
    let mut iov = libc::iovec {
        iov_base: payload.as_mut_ptr() as *mut libc::c_void,
        iov_len: payload.len(),
    };
    let mut msg: libc::msghdr = unsafe { mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control.as_mut_ptr() as *mut libc::c_void;
    msg.msg_controllen = control.len();
    let n = unsafe { libc::recvmsg(stream.as_raw_fd(), &mut msg, 0) };
    if n < 0 {
        return Err(io::Error::last_os_error().into());
    }
    if msg.msg_flags & libc::MSG_CTRUNC != 0 || msg.msg_flags & libc::MSG_TRUNC != 0 {
        return Err(
            io::Error::new(io::ErrorKind::InvalidData, "truncated Smartmap response").into(),
        );
    }

    let resp: T = serde_json::from_slice(&payload[..n as usize])?;
    let mut fds = Vec::new();
    unsafe {
        let mut cmsg = libc::CMSG_FIRSTHDR(&msg);
        while !cmsg.is_null() {
            if (*cmsg).cmsg_level == libc::SOL_SOCKET && (*cmsg).cmsg_type == libc::SCM_RIGHTS {
                let len = (*cmsg).cmsg_len as usize - cmsg_len(0);
                let count = len / mem::size_of::<RawFd>();
                let data = libc::CMSG_DATA(cmsg) as *const RawFd;
                for i in 0..count {
                    fds.push(OwnedFd::from_raw_fd(*data.add(i)));
                }
            }
            cmsg = libc::CMSG_NXTHDR(&msg, cmsg);
        }
    }
    Ok((resp, fds))
}

fn cmsg_space(len: usize) -> usize {
    unsafe { libc::CMSG_SPACE(len as u32) as usize }
}

fn cmsg_len(len: usize) -> usize {
    unsafe { libc::CMSG_LEN(len as u32) as usize }
}

use std::os::fd::AsRawFd;
