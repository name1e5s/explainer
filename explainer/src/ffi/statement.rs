use std::{
    ffi::{c_int, CStr},
    ptr::NonNull,
};

use libsqlite3_sys::{
    sqlite3, sqlite3_bind_parameter_count, sqlite3_column_count, sqlite3_column_database_name,
    sqlite3_column_origin_name, sqlite3_column_table_name, sqlite3_db_handle, sqlite3_finalize,
    sqlite3_step, sqlite3_stmt, sqlite3_stmt_readonly, sqlite3_table_column_metadata, SQLITE_DONE,
    SQLITE_OK, SQLITE_ROW,
};

use crate::types::{ColumnType, DataType};

use super::error::SqliteError;

pub struct Statement(NonNull<sqlite3_stmt>);

unsafe impl Send for Statement {}

impl Statement {
    pub fn new(handle: NonNull<sqlite3_stmt>) -> Statement {
        Statement(handle)
    }

    pub fn db_handle(&self) -> *mut sqlite3 {
        unsafe { sqlite3_db_handle(self.0.as_ptr()) }
    }

    pub fn as_ptr(&self) -> *mut sqlite3_stmt {
        self.0.as_ptr()
    }

    pub fn read_only(&self) -> bool {
        unsafe { sqlite3_stmt_readonly(self.0.as_ptr()) != 0 }
    }

    pub fn bind_parameter_count(&self) -> usize {
        unsafe { sqlite3_bind_parameter_count(self.0.as_ptr()) as usize }
    }

    pub fn column_count(&self) -> usize {
        unsafe { sqlite3_column_count(self.0.as_ptr()) as usize }
    }

    pub fn column_type(&self, index: usize) -> anyhow::Result<Option<ColumnType>> {
        unsafe {
            // https://sqlite.org/c3ref/column_database_name.html
            //
            // ### Note
            // The returned string is valid until the prepared statement is destroyed using
            // sqlite3_finalize() or until the statement is automatically reprepared by the
            // first call to sqlite3_step() for a particular run or until the same information
            // is requested again in a different encoding.
            let db_name = sqlite3_column_database_name(self.0.as_ptr(), index as c_int);
            let table_name = sqlite3_column_table_name(self.0.as_ptr(), index as c_int);
            let origin_name = sqlite3_column_origin_name(self.0.as_ptr(), index as c_int);

            if db_name.is_null() || table_name.is_null() || origin_name.is_null() {
                return Ok(None);
            }

            let mut not_null: c_int = 0;
            let mut data_type = std::ptr::null();

            // https://sqlite.org/c3ref/table_column_metadata.html
            let status = sqlite3_table_column_metadata(
                self.db_handle(),
                db_name,
                table_name,
                origin_name,
                // function docs state to provide NULL for return values you don't care about
                &mut data_type,
                std::ptr::null_mut(),
                &mut not_null,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            );

            if status != SQLITE_OK {
                // implementation note: the docs for sqlite3_table_column_metadata() specify
                // that an error can be returned if the column came from a view; however,
                // experimentally we found that the above functions give us the true origin
                // for columns in views that came from real tables and so we should never hit this
                // error; for view columns that are expressions we are given NULL for their origins
                // so we don't need special handling for that case either.
                //
                // this is confirmed in the `tests/sqlite-macros.rs` integration test
                return Err(SqliteError::new(self.db_handle()).into());
            }

            if data_type.is_null() {
                return Ok(None);
            }

            let data_type: DataType = CStr::from_ptr(data_type).to_str()?.parse()?;

            Ok(Some(ColumnType {
                data_type,
                nullable: not_null == 0,
            }))
        }
    }

    pub fn step(&mut self) -> Result<bool, SqliteError> {
        unsafe {
            match sqlite3_step(self.0.as_ptr()) {
                SQLITE_ROW => Ok(true),
                SQLITE_DONE => Ok(false),
                _ => Err(SqliteError::new(self.db_handle())),
            }
        }
    }
}

impl Drop for Statement {
    fn drop(&mut self) {
        unsafe {
            let status = sqlite3_finalize(self.0.as_ptr());
            if status != SQLITE_OK {
                let msg = format!("sqlite3_finalize failed: {status}");
                eprintln!("{msg}");
                panic!("{}", msg);
            }
        }
    }
}