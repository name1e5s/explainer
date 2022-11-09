use libsqlite3_sys::{sqlite3, sqlite3_errmsg, sqlite3_extended_errcode};
use std::error::Error;
use std::{
    ffi::CStr,
    fmt::{self, Display, Formatter},
    os::raw::c_int,
    str::from_utf8_unchecked,
};

#[derive(Debug)]
pub struct SqliteError {
    code: i32,
    message: String,
}

impl SqliteError {
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    pub fn new(handle: *mut sqlite3) -> Self {
        let code: c_int = unsafe { sqlite3_extended_errcode(handle) };
        let message = unsafe {
            let msg = sqlite3_errmsg(handle);
            debug_assert!(!msg.is_null());

            from_utf8_unchecked(CStr::from_ptr(msg).to_bytes())
        };

        Self {
            code,
            message: message.to_owned(),
        }
    }
}

impl Display for SqliteError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "(code: {}) {}", self.code, self.message)
    }
}

impl Error for SqliteError {}
