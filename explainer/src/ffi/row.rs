use libsqlite3_sys::{
    sqlite3_column_blob, sqlite3_column_bytes, sqlite3_column_double, sqlite3_column_int,
    sqlite3_column_int64,
};

use super::statement::Statement;

pub struct Row<'a>(&'a Statement);

impl<'a> Row<'a> {
    pub fn new(statement: &'a Statement) -> Row<'a> {
        Row(statement)
    }

    pub fn column_count(&self) -> usize {
        self.0.column_count()
    }

    pub fn column_int(&self, index: usize) -> i32 {
        unsafe { sqlite3_column_int(self.0.as_ptr(), index as i32) }
    }

    pub fn column_int64(&self, index: usize) -> i64 {
        unsafe { sqlite3_column_int64(self.0.as_ptr(), index as i32) }
    }

    pub fn column_double(&self, index: usize) -> f64 {
        unsafe { sqlite3_column_double(self.0.as_ptr(), index as i32) }
    }

    pub fn column_blob(&self, index: usize) -> &[u8] {
        let index = index as i32;
        let len = unsafe { sqlite3_column_bytes(self.0.as_ptr(), index) } as usize;

        if len == 0 {
            // empty blobs are NULL so just return an empty slice
            return &[];
        }

        let ptr = unsafe { sqlite3_column_blob(self.0.as_ptr(), index) } as *const u8;
        debug_assert!(!ptr.is_null());

        unsafe { std::slice::from_raw_parts(ptr, len) }
    }

    pub fn column_text(&self, index: usize) -> &str {
        std::str::from_utf8(self.column_blob(index)).unwrap_or("")
    }
}
