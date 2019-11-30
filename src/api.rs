use std::ffi::CString;
use std::os::raw::c_char;
use std::slice;

use arrow::ipc;
use arrow::ipc::convert;

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

#[allow(clippy::missing_safety_doc)]
#[no_mangle]
pub extern "C" fn print_schema(schema_fb: *const u8, schema_len: usize) {
    let schema_buffer = unsafe { slice::from_raw_parts(schema_fb, schema_len) };
    let schema_fb = ipc::get_root_as_message(schema_buffer)
        .header_as_schema()
        .expect("Not a schema message");
    let schema = convert::fb_to_schema(schema_fb);
    println!("{:#?}", schema);
}
