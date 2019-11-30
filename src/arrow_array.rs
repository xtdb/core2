// https://github.com/pitrou/arrow/blob/doc-c-protocol/docs/source/format/CDataInterface.rst
// https://github.com/pitrou/arrow/blob/c-data-interface-impl/cpp/src/arrow/c/bridge.cc
// Apache License 2.0

// #include <stdint.h>

// #define ARROW_FLAG_ORDERED 1
// #define ARROW_FLAG_NULLABLE 2
// #define ARROW_FLAG_KEYS_SORTED 4

// struct ArrowArray {
//   // Type description
//   const char* format;
//   const char* name;
//   const char* metadata;
//   int64_t flags;

//   // Data description
//   int64_t length;
//   int64_t null_count;
//   int64_t offset;
//   int64_t n_buffers;
//   int64_t n_children;
//   const void** buffers;
//   struct ArrowArray** children;
//   struct ArrowArray* dictionary;

//   // Release callback
//   void (*release)(struct ArrowArray*);
//   // Opaque producer-specific data
//   void* private_data;
// };

/* automatically generated by rust-bindgen */

// slightly edited and trimmed down after running:
// cargo install bindgen
// bindgen arrow_array.h -o src/arrow_array.rs

pub const ARROW_FLAG_ORDERED: u32 = 1;
pub const ARROW_FLAG_NULLABLE: u32 = 2;
pub const ARROW_FLAG_KEYS_SORTED: u32 = 4;

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct ArrowArray {
    pub format: *const ::std::os::raw::c_char,
    pub name: *const ::std::os::raw::c_char,
    pub metadata: *const ::std::os::raw::c_char,
    pub flags: i64,
    pub length: i64,
    pub null_count: i64,
    pub offset: i64,
    pub n_buffers: i64,
    pub n_children: i64,
    pub buffers: *mut *const ::std::os::raw::c_void,
    pub children: *mut *mut ArrowArray,
    pub dictionary: *mut ArrowArray,
    pub release: ::std::option::Option<unsafe extern "C" fn(arg1: *mut ArrowArray)>,
    pub private_data: *mut ::std::os::raw::c_void,
}
