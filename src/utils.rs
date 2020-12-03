// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::ops::Bound;

use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::*;

pub fn to_py_execption(err: impl std::fmt::Display) -> PyErr {
    PyException::new_err(format!("{}", err))
}

// pub fn from_py_bytes(bytes: Py<PyBytes>) -> Vec<u8> {
//     Python::with_gil(|py| bytes.as_ref(py).as_bytes().to_vec())
// }

pub fn to_py_bytes(bytes: Vec<u8>) -> Py<PyBytes> {
    Python::with_gil(|py| PyBytes::new(py, &bytes).into())
}

// pub fn from_py_key_list(list: Py<PyList>) -> PyResult<Vec<tikv_client::Key>> {
//     Python::with_gil(|py| {
//         let mut results = Vec::new();
//         for item in list.as_ref(py) {
//             let bytes = item.downcast::<PyBytes>()?;
//             results.push(bytes.as_bytes().to_vec().into());
//         }
//         Ok(results)
//     })
// }

pub fn to_py_key_list(keys: impl Iterator<Item = tikv_client::Key>) -> PyResult<Py<PyList>> {
    Python::with_gil(|py| {
        let list = PyList::empty(py);
        for key in keys {
            list.append(PyBytes::new(py, (&key).into()))?;
        }
        Ok(list.into())
    })
}

pub fn to_py_kv_list(pairs: impl IntoIterator<Item = tikv_client::KvPair>) -> PyResult<Py<PyList>> {
    Python::with_gil(|py| {
        let list = PyList::empty(py);
        for (key, val) in pairs.into_iter().map(Into::into) {
            let key: Py<PyBytes> = PyBytes::new(py, (&key).into()).into();
            let val: Py<PyBytes> = PyBytes::new(py, &val).into();
            list.append(PyTuple::new(py, &[key, val]))?;
        }
        Ok(list.into())
    })
}

pub fn from_py_dict(dict: Py<PyDict>) -> PyResult<Vec<tikv_client::KvPair>> {
    Python::with_gil(|py| {
        let mut pairs = Vec::new();
        for (key, val) in dict.as_ref(py).into_iter() {
            let key = key.downcast::<PyBytes>()?;
            let val = val.downcast::<PyBytes>()?;
            pairs.push(tikv_client::KvPair::new(
                key.as_bytes().to_owned(),
                val.as_bytes().to_owned(),
            ));
        }
        Ok(pairs)
    })
}

pub fn to_bound_range(
    start: Option<Vec<u8>>,
    end: Option<Vec<u8>>,
    include_start: bool,
    include_end: bool,
) -> tikv_client::BoundRange {
    let start_bound = if let Some(start) = start {
        if include_start {
            Bound::Included(start)
        } else {
            Bound::Excluded(start)
        }
    } else {
        Bound::Unbounded
    };
    let end_bound = if let Some(end) = end {
        if include_end {
            Bound::Included(end)
        } else {
            Bound::Excluded(end)
        }
    } else {
        Bound::Unbounded
    };
    tikv_client::BoundRange::from((start_bound, end_bound))
}
