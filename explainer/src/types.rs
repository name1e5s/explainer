use std::str::FromStr;

use anyhow::bail;
use libsqlite3_sys::{SQLITE_BLOB, SQLITE_FLOAT, SQLITE_INTEGER, SQLITE_TEXT};

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum DataType {
    Null,
    Bool,
    Int,
    BigInt,
    Real,
    Text,
    Blob,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct ColumnType {
    pub datatype: DataType,
    pub nullable: Option<bool>,
}

impl ColumnType {
    pub fn from_type_code(type_code: i32) -> ColumnType {
        match type_code {
            SQLITE_INTEGER => Some(DataType::Int),
            SQLITE_FLOAT => Some(DataType::Real),
            SQLITE_TEXT => Some(DataType::Text),
            SQLITE_BLOB => Some(DataType::Blob),
            _ => None,
        }
        .map(|v| ColumnType {
            datatype: v,
            // SqliteType can generate T and Option<T>
            nullable: Some(false),
        })
        .unwrap_or(ColumnType {
            datatype: DataType::Null,
            nullable: Some(true),
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Constraint {
    Count(usize),
    Types(Vec<ColumnType>),
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct StatementInfo {
    pub read_only: bool,
    pub input_length: usize,
    pub output_length: usize,
    pub output_types: Vec<Option<ColumnType>>,
}

impl FromStr for DataType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_ascii_lowercase();
        Ok(match &*s {
            "int4" => DataType::Int,
            "int8" => DataType::BigInt,
            "boolean" | "bool" => DataType::Bool,

            _ if s.contains("int") => {
                if s.contains("big") {
                    DataType::BigInt
                } else {
                    DataType::Int
                }
            }

            _ if s.contains("char") || s.contains("clob") || s.contains("text") => DataType::Text,

            _ if s.contains("blob") => DataType::Blob,

            _ if s.contains("real") || s.contains("floa") || s.contains("doub") => DataType::Real,

            _ => {
                bail!("unknown type: `{}`", s);
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_type_from_str() -> anyhow::Result<()> {
        assert_eq!(DataType::Int, "INT4".parse()?);

        assert_eq!(DataType::Int, "INT".parse()?);
        assert_eq!(DataType::Int, "INTEGER".parse()?);
        assert_eq!(DataType::Int, "MEDIUMINT".parse()?);

        assert_eq!(DataType::BigInt, "INTBIG".parse()?);
        assert_eq!(DataType::BigInt, "BIGINT".parse()?);
        assert_eq!(DataType::BigInt, "UNSIGNED BIG INT".parse()?);
        assert_eq!(DataType::BigInt, "INT8".parse()?);

        assert_eq!(DataType::Text, "CHARACTER(20)".parse()?);
        assert_eq!(DataType::Text, "NCHAR(55)".parse()?);
        assert_eq!(DataType::Text, "TEXT".parse()?);
        assert_eq!(DataType::Text, "CLOB".parse()?);

        assert_eq!(DataType::Blob, "BLOB".parse()?);

        assert_eq!(DataType::Real, "REAL".parse()?);
        assert_eq!(DataType::Real, "FLOAT".parse()?);
        assert_eq!(DataType::Real, "DOUBLE PRECISION".parse()?);

        assert_eq!(DataType::Bool, "BOOLEAN".parse()?);
        assert_eq!(DataType::Bool, "BOOL".parse()?);

        Ok(())
    }
}
