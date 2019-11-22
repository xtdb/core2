use std::ffi::CString;
use std::os::raw::c_char;

#[no_mangle]
pub extern "C" fn c_version_string() -> *const c_char {
    CString::new(crate::config::version_string())
        .expect("Unexpected NULL")
        .into_raw()
}

#[allow(clippy::missing_safety_doc)]
#[no_mangle]
pub unsafe extern "C" fn c_string_free(c_string: *mut c_char) {
    let _ = CString::from_raw(c_string);
}
