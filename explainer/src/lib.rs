use std::ffi::{CStr, CString};

use ffi::connection::Connection;
use types::StatementInfo;

pub mod explain;
pub mod ffi;
pub mod utils;

pub mod types;

pub fn add(left: usize, right: usize) -> usize {
    left + right
}

pub fn get_statement_info(db_path: &CStr, sql: &str) -> anyhow::Result<StatementInfo> {
    let conn = Connection::establish(db_path)?;
    let sql_c = {
        let mut sql = sql.to_string();
        sql.push('\0');
        CString::from_vec_with_nul(sql.into_bytes())?
    };
    let stmt = conn.prepare(&sql_c)?;
    let read_only = stmt.read_only();
    let parameter_count = stmt.bind_parameter_count();

    // output types
    let column_count = stmt.column_count();
    let mut has_undecided_datatype = false;

    // t1: get types from db directly
    let mut column_types = (0..column_count)
        .map(|i| {
            let column_type = stmt.column_database_type(i)?;
            if column_type.is_none() {
                has_undecided_datatype = true;
            }
            Ok(column_type)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    // t2: get types from explain
    if has_undecided_datatype {
        let explain_column_types = explain::explain(&conn, sql)?;
        for (i, column_type) in column_types.iter_mut().enumerate() {
            if column_type.is_none() {
                *column_type = explain_column_types.get(i).cloned();
            }
        }
    }

    Ok(StatementInfo {
        read_only,
        input_length: parameter_count,
        output_length: column_count,
        output_types: column_types,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
