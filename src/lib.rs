#![feature(try_blocks)]

mod pycoroutine;
mod raw;
mod transaction;
mod utils;

use pyo3::prelude::*;

#[pymodule]
fn tikv_client(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<raw::RawClient>()?;
    m.add_class::<transaction::TransactionClient>()?;
    Ok(())
}
