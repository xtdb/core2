// See https://github.com/apache/arrow/pull/6026/files

// cpp/src/arrow/c/abi.h
// cpp/src/arrow/c/bridge.h
// cpp/src/arrow/c/bridge.cc

// // Licensed to the Apache Software Foundation (ASF) under one
// // or more contributor license agreements.  See the NOTICE file
// // distributed with this work for additional information
// // regarding copyright ownership.  The ASF licenses this file
// // to you under the Apache License, Version 2.0 (the
// // "License"); you may not use this file except in compliance
// // with the License.  You may obtain a copy of the License at
// //
// //   http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing,
// // software distributed under the License is distributed on an
// // "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// // KIND, either express or implied.  See the License for the
// // specific language governing permissions and limitations
// // under the License.

// #pragma once

// #include <stdint.h>

// #ifdef __cplusplus
// extern "C" {
// #endif

// #define ARROW_FLAG_DICTIONARY_ORDERED 1
// #define ARROW_FLAG_NULLABLE 2
// #define ARROW_FLAG_MAP_KEYS_SORTED 4

// struct ArrowSchema {
//   // Array type description
//   const char* format;
//   const char* name;
//   const char* metadata;
//   int64_t flags;
//   int64_t n_children;
//   struct ArrowSchema** children;
//   struct ArrowSchema* dictionary;

//   // Release callback
//   void (*release)(struct ArrowSchema*);
//   // Opaque producer-specific data
//   void* private_data;
// };

// struct ArrowArray {
//   // Array data description
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

// #ifdef __cplusplus
// }
// #endif

pub const ARROW_FLAG_DICTIONARY_ORDERED: u32 = 1;
pub const ARROW_FLAG_NULLABLE: u32 = 2;
pub const ARROW_FLAG_MAP_KEYS_SORTED: u32 = 4;

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct ArrowSchema {
    pub format: *const ::std::os::raw::c_char,
    pub name: *const ::std::os::raw::c_char,
    pub metadata: *const ::std::os::raw::c_char,
    pub flags: i64,
    pub n_children: i64,
    pub children: *mut *mut ArrowSchema,
    pub dictionary: *mut ArrowSchema,
    pub release: ::std::option::Option<unsafe extern "C" fn(arg1: *mut ArrowSchema)>,
    pub private_data: *mut ::std::os::raw::c_void,
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct ArrowArray {
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
