mod pycoroutine;

use std::convert::TryInto;
use std::ops::Bound;
use std::sync::Arc;

use futures::executor::block_on;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::*;
use tokio::sync::RwLock;

use crate::pycoroutine::PyCoroutine;

#[pymodule]
fn tikv_client(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyCoroutine>()?;
    m.add_class::<Client>()?;
    m.add_class::<Transaction>()?;
    Ok(())
}

#[pyclass]
struct Client {
    inner: Arc<tikv_client::TransactionClient>,
}

#[pymethods]
impl Client {
    #[new]
    fn new(pd_endpoint: String) -> PyResult<Self> {
        let client = block_on(tikv_client::TransactionClient::new(
            tikv_client::Config::new(vec![pd_endpoint]),
        ))
        .map_err(to_py_execption)?;
        Ok(Client {
            inner: Arc::new(client),
        })
    }

    fn begin(&self) -> PyCoroutine {
        let inner = self.inner.clone();
        PyCoroutine::new(async move {
            let transaction = inner.begin().await.map_err(to_py_execption)?;
            Ok(Transaction {
                inner: Arc::new(RwLock::new(transaction)),
            })
        })
    }
}

#[pyclass]
struct Transaction {
    inner: Arc<RwLock<tikv_client::Transaction>>,
}

#[pymethods]
impl Transaction {
    fn get(&self, key: Vec<u8>) -> PyCoroutine {
        let inner = self.inner.clone();
        PyCoroutine::new(async move {
            let val = inner
                .read()
                .await
                .get(key)
                .await
                .map_err(to_py_execption)?
                .map(to_py_bytes);
            Ok(val)
        })
    }

    fn get_for_update(&self, key: Vec<u8>) -> PyCoroutine {
        let inner = self.inner.clone();
        PyCoroutine::new(async move {
            let val = inner
                .write()
                .await
                .get_for_update(key)
                .await
                .map_err(to_py_execption)?
                .map(to_py_bytes);
            Ok(val)
        })
    }

    fn batch_get(&self, keys: Vec<Vec<u8>>) -> PyCoroutine {
        let inner = self.inner.clone();
        PyCoroutine::new(async move {
            let kv_pairs = inner
                .read()
                .await
                .batch_get(keys)
                .await
                .map_err(to_py_execption)?;
            let py_dict = to_py_dict(kv_pairs)?;
            Ok(py_dict)
        })
    }

    fn batch_get_for_update(&self, keys: Vec<Vec<u8>>) -> PyCoroutine {
        let inner = self.inner.clone();
        PyCoroutine::new(async move {
            let kv_pairs = inner
                .write()
                .await
                .batch_get_for_update(keys)
                .await
                .map_err(to_py_execption)?;
            let py_dict = to_py_dict(kv_pairs)?;
            Ok(py_dict)
        })
    }

    #[args(
        limit = 1,
        include_start = "true",
        include_end = "false",
    )]
    fn scan(
        &self,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        limit: u32,
        include_start: bool,
        include_end: bool,
    ) -> PyCoroutine {
        let inner = self.inner.clone();
        PyCoroutine::new(async move {
            let start_bound = if include_start {
                Bound::Included(start)
            } else {
                Bound::Excluded(start)
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
            let range: tikv_client::BoundRange = (start_bound, end_bound)
                .try_into()
                .map_err(to_py_execption)?;

            let kv_pairs = inner
                .read()
                .await
                .scan(range, limit)
                .await
                .map_err(to_py_execption)?;
            let py_dict = to_py_dict(kv_pairs)?;
            Ok(py_dict)
        })
    }

    fn lock_keys(&self, keys: Vec<Vec<u8>>) -> PyCoroutine {
        let inner = self.inner.clone();
        PyCoroutine::new(async move {
            inner
                .read()
                .await
                .lock_keys(keys)
                .await
                .map_err(to_py_execption)?;
            Ok(())
        })
    }

    fn put(&self, key: Vec<u8>, val: Vec<u8>) -> PyCoroutine {
        let inner = self.inner.clone();
        PyCoroutine::new(async move {
            inner
                .write()
                .await
                .put(key, val)
                .await
                .map_err(to_py_execption)?;
            Ok(())
        })
    }

    fn delete(&self, key: Vec<u8>) -> PyCoroutine {
        let inner = self.inner.clone();
        PyCoroutine::new(async move {
            inner
                .write()
                .await
                .delete(key)
                .await
                .map_err(to_py_execption)?;
            Ok(())
        })
    }

    fn commit(&self) -> PyCoroutine {
        let inner = self.inner.clone();
        PyCoroutine::new(async move { inner.write().await.commit().await.map_err(to_py_execption) })
    }
}

fn to_py_execption(err: impl std::fmt::Display) -> PyErr {
    PyException::new_err(format!("{}", err))
}

// fn from_py_bytes(bytes: Py<PyBytes>) -> Vec<u8> {
//     Python::with_gil(|py| bytes.as_ref(py).as_bytes().to_vec())
// }

fn to_py_bytes(bytes: Vec<u8>) -> Py<PyBytes> {
    Python::with_gil(|py| PyBytes::new(py, &bytes).into())
}

// fn from_py_key_list(list: Py<PyList>) -> PyResult<Vec<tikv_client::Key>> {
//     Python::with_gil(|py| {
//         let mut results = Vec::new();
//         for item in list.as_ref(py) {
//             let bytes = item.downcast::<PyBytes>()?;
//             results.push(bytes.as_bytes().to_vec().into());
//         }
//         Ok(results)
//     })
// }

fn to_py_dict(kv_pairs: impl Iterator<Item = tikv_client::KvPair>) -> PyResult<Py<PyDict>> {
    Python::with_gil(|py| {
        let dict = PyDict::new(py);
        for (key, val) in kv_pairs.into_iter().map(Into::into) {
            let key: Py<PyBytes> = PyBytes::new(py, (&key).into()).into();
            let val: Py<PyBytes> = PyBytes::new(py, &val).into();
            dict.set_item(key, val)?;
        }
        Ok(dict.into())
    })
}
