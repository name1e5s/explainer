use libsqlite3_sys::{sqlite3, sqlite3_close, sqlite3_open, sqlite3_prepare_v2, SQLITE_OK};
use std::{ffi::CStr, ptr::NonNull};

use crate::ffi::error::SqliteError;

use super::{row::Row, statement::Statement};

#[derive(Debug)]
pub struct Connection(NonNull<sqlite3>);

impl Connection {
    pub fn establish(path: &CStr) -> Result<Connection, SqliteError> {
        let mut handle = std::ptr::null_mut();
        let _ = unsafe { sqlite3_open(path.as_ptr(), &mut handle) };
        if handle.is_null() {
            return Err(SqliteError::new(handle));
        }
        let connection = unsafe { Connection(NonNull::new_unchecked(handle)) };
        Ok(connection)
    }

    pub fn as_ptr(&self) -> *mut sqlite3 {
        self.0.as_ptr()
    }

    pub fn prepare(&self, sql: &CStr) -> Result<Statement, SqliteError> {
        let mut handle = std::ptr::null_mut();
        let status = unsafe {
            sqlite3_prepare_v2(
                self.as_ptr(),
                sql.as_ptr(),
                -1,
                &mut handle,
                std::ptr::null_mut(),
            )
        };
        if handle.is_null() || status != SQLITE_OK {
            return Err(SqliteError::new(self.as_ptr()));
        }
        let statement = unsafe { Statement::new(NonNull::new_unchecked(handle)) };
        Ok(statement)
    }

    pub fn exec(
        &self,
        query: &CStr,
        mut f: Option<&mut dyn FnMut(&Row) -> bool>,
    ) -> Result<(), SqliteError> {
        let mut stmt = self.prepare(query)?;
        let mut cont = true;
        while cont && stmt.step()? {
            if let Some(f) = &mut f {
                cont = f(&Row::new(&stmt));
            }
        }
        Ok(())
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        unsafe {
            let ret = sqlite3_close(self.0.as_ptr());
            if ret != SQLITE_OK {
                let msg = format!(
                    "sqlite3_close failed: {}",
                    SqliteError::new(self.0.as_ptr())
                );
                eprintln!("{msg}");
                panic!("{}", msg);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    #[test]
    fn test_open() {
        let path = CString::new(":memory:").unwrap();
        let conn = Connection::establish(&path).unwrap();
        assert!(!conn.as_ptr().is_null());
    }

    #[test]
    fn test_prepare() {
        let path = CString::new(":memory:").unwrap();
        let conn = Connection::establish(&path).unwrap();
        let query = CString::new("SELECT 1").unwrap();
        let stmt = conn.prepare(&query).unwrap();
        assert!(!stmt.as_ptr().is_null());
    }

    #[test]
    fn test_exec() {
        let path = CString::new(":memory:").unwrap();
        let conn = Connection::establish(&path).unwrap();
        let query = CString::new("SELECT 1").unwrap();
        let mut count = 0;
        conn.exec(
            &query,
            Some(&mut |row| {
                count += 1;
                assert_eq!(row.column_count(), 1);
                assert_eq!(row.column_int(0), 1);
                false
            }),
        )
        .unwrap();
        assert_eq!(count, 1);
    }
}
