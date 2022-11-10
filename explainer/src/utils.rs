use std::ffi::CStr;

/// Get static CStr from a string literal
/// 
/// NUL bytes in the string literal are not allowed.
/// # Example
/// 
/// ```
/// # use explainer::cstr;
/// let cstr = cstr!("Hello, world!");
/// ```
/// 
/// ```compile_fail
/// # use explainer::cstr;
/// let cstr = cstr!("Hello, \0world!");
/// ```
#[macro_export]
macro_rules! cstr {
    ($s:literal) => {{
        static __CSTR: &'static std::ffi::CStr = $crate::utils::static_cstr(concat!($s, "\0"));
        __CSTR
    }};
}

#[doc(hidden)]
pub const fn static_cstr(input: &'static str) -> &'static CStr {
    let bytes = input.as_bytes();
    let len = bytes.len();
    let mut index = 0;
    while index < len - 1 {
        if bytes[index] == 0 {
            panic!("NUL byte in static string");
        }
        index += 1;
    }
    if bytes[index] != 0 {
        panic!("missing NUL byte in static string");
    }
    unsafe { CStr::from_bytes_with_nul_unchecked(input.as_bytes()) }
}
