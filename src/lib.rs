// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(try_blocks)]
#![feature(never_type)]

mod raw;
mod transaction;
mod utils;

use pyo3::prelude::*;

#[pymodule]
fn tikv_client(_py: Python, m: &PyModule) -> PyResult<()> {
    unsafe {
        pyo3::ffi::PyEval_InitThreads();
    }
    m.add_class::<raw::RawClient>()?;
    m.add_class::<transaction::TransactionClient>()?;
    Ok(())
}
