// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::*;
use pyo3::ToPyObject;
use pyo3_asyncio::tokio::future_into_py;
use tikv_client::TimestampExt as _;
use tikv_client::TransactionOptions;
use tokio::sync::RwLock;

use crate::utils::*;

#[pyclass]
pub struct TransactionClient {
    inner: Arc<tikv_client::TransactionClient>,
}

#[pymethods]
impl TransactionClient {
    #[new]
    pub fn new() -> PyResult<Self> {
        Err(PyException::new_err(
            "Please use `TransactionClient.connect()` instead.",
        ))
    }

    #[classmethod]
    pub fn connect<'p>(_cls: &PyType, py: Python<'p>, pd_endpoint: String) -> PyResult<&'p PyAny> {
        future_into_py(py, async move {
            let inner = tikv_client::TransactionClient::new(vec![pd_endpoint], None)
                .await
                .map_err(to_py_execption)?;
            let client = TransactionClient {
                inner: Arc::new(inner),
            };
            Python::with_gil(|py| Ok(PyCell::new(py, client)?.to_object(py)))
        })
    }

    #[args(pessimistic = "false")]
    pub fn begin<'p>(&self, py: Python<'p>, pessimistic: bool) -> PyResult<&'p PyAny> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let transaction = if pessimistic {
                inner.begin_pessimistic().await.map_err(to_py_execption)?
            } else {
                inner.begin_optimistic().await.map_err(to_py_execption)?
            };
            let transaction = Transaction {
                inner: Arc::new(RwLock::new(transaction)),
            };
            Python::with_gil(|py| PyCell::new(py, transaction).map(|py_cell| py_cell.to_object(py)))
        })
    }

    pub fn current_timestamp<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let timestamp = inner
                .current_timestamp()
                .await
                .map_err(to_py_execption)?
                .version();
            Ok(Python::with_gil(|py| timestamp.to_object(py)))
        })
    }

    #[args(pessimistic = "false")]
    pub fn snapshot(&self, timestamp: u64, pessimistic: bool) -> Snapshot {
        Snapshot {
            inner: Arc::new(RwLock::new(self.inner.snapshot(
                tikv_client::Timestamp::from_version(timestamp),
                if pessimistic {
                    TransactionOptions::new_pessimistic()
                } else {
                    TransactionOptions::new_optimistic()
                },
            ))),
        }
    }

    pub fn gc<'p>(&self, py: Python<'p>, safepoint: u64) -> PyResult<&'p PyAny> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let result = inner
                .gc(tikv_client::Timestamp::from_version(safepoint))
                .await
                .map_err(to_py_execption)?;
            Ok(Python::with_gil(|py| result.to_object(py)))
        })
    }
}

#[pyclass]
pub struct Snapshot {
    inner: Arc<RwLock<tikv_client::Snapshot>>,
}

#[pymethods]
impl Snapshot {
    pub fn get<'p>(&self, py: Python<'p>, key: Vec<u8>) -> PyResult<&'p PyAny> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let val = inner
                .write()
                .await
                .get(key)
                .await
                .map_err(to_py_execption)?
                .map(to_py_bytes);
            Ok(Python::with_gil(|py| val.to_object(py)))
        })
    }

    pub fn key_exists<'p>(&self, py: Python<'p>, key: Vec<u8>) -> PyResult<&'p PyAny> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let val = inner
                .write()
                .await
                .key_exists(key)
                .await
                .map_err(to_py_execption)?;
            Ok(Python::with_gil(|py| val.to_object(py)))
        })
    }

    pub fn batch_get<'p>(&self, py: Python<'p>, keys: Vec<Vec<u8>>) -> PyResult<&'p PyAny> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let kv_pairs = inner
                .write()
                .await
                .batch_get(keys)
                .await
                .map_err(to_py_execption)?;
            let py_list = to_py_kv_list(kv_pairs)?;
            Ok(Python::with_gil(|py| py_list.to_object(py)))
        })
    }

    #[args(include_start = "true", include_end = "false")]
    pub fn scan<'p>(
        &self,
        py: Python<'p>,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        limit: u32,
        include_start: bool,
        include_end: bool,
    ) -> PyResult<&'p PyAny> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let range = to_bound_range(start, end, include_start, include_end);
            let kv_pairs = inner
                .write()
                .await
                .scan(range, limit)
                .await
                .map_err(to_py_execption)?;
            let py_list = to_py_kv_list(kv_pairs)?;
            Ok(Python::with_gil(|py| py_list.to_object(py)))
        })
    }

    #[args(include_start = "true", include_end = "false")]
    pub fn scan_keys<'p>(
        &self,
        py: Python<'p>,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        limit: u32,
        include_start: bool,
        include_end: bool,
    ) -> PyResult<&'p PyAny> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let range = to_bound_range(start, end, include_start, include_end);
            let keys = inner
                .write()
                .await
                .scan_keys(range, limit)
                .await
                .map_err(to_py_execption)?;
            let py_list = to_py_key_list(keys)?;
            Ok(Python::with_gil(|py| py_list.to_object(py)))
        })
    }
}

#[pyclass]
pub struct Transaction {
    inner: Arc<RwLock<tikv_client::Transaction>>,
}

#[pymethods]
impl Transaction {
    pub fn get<'p>(&self, py: Python<'p>, key: Vec<u8>) -> PyResult<&'p PyAny> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let val = inner
                .write()
                .await
                .get(key)
                .await
                .map_err(to_py_execption)?
                .map(to_py_bytes);
            Ok(Python::with_gil(|py| val.to_object(py)))
        })
    }

    pub fn get_for_update<'p>(&self, py: Python<'p>, key: Vec<u8>) -> PyResult<&'p PyAny> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let val = inner
                .write()
                .await
                .get_for_update(key)
                .await
                .map_err(to_py_execption)?
                .map(to_py_bytes);
            Ok(Python::with_gil(|py| val.to_object(py)))
        })
    }

    pub fn key_exists<'p>(&self, py: Python<'p>, key: Vec<u8>) -> PyResult<&'p PyAny> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let val = inner
                .write()
                .await
                .key_exists(key)
                .await
                .map_err(to_py_execption)?;
            Ok(Python::with_gil(|py| val.to_object(py)))
        })
    }

    pub fn batch_get<'p>(&self, py: Python<'p>, keys: Vec<Vec<u8>>) -> PyResult<&'p PyAny> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let kv_pairs = inner
                .write()
                .await
                .batch_get(keys)
                .await
                .map_err(to_py_execption)?;
            let py_list = to_py_kv_list(kv_pairs)?;
            Ok(Python::with_gil(|py| py_list.to_object(py)))
        })
    }

    pub fn batch_get_for_update<'p>(
        &self,
        py: Python<'p>,
        keys: Vec<Vec<u8>>,
    ) -> PyResult<&'p PyAny> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let kv_pairs = inner
                .write()
                .await
                .batch_get_for_update(keys)
                .await
                .map_err(to_py_execption)?;
            let py_list = to_py_kv_list(kv_pairs)?;
            Ok(Python::with_gil(|py| py_list.to_object(py)))
        })
    }

    #[args(include_start = "true", include_end = "false")]
    pub fn scan<'p>(
        &self,
        py: Python<'p>,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        limit: u32,
        include_start: bool,
        include_end: bool,
    ) -> PyResult<&'p PyAny> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let range = to_bound_range(start, end, include_start, include_end);
            let kv_pairs = inner
                .write()
                .await
                .scan(range, limit)
                .await
                .map_err(to_py_execption)?;
            let py_list = to_py_kv_list(kv_pairs)?;
            Ok(Python::with_gil(|py| py_list.to_object(py)))
        })
    }

    #[args(include_start = "true", include_end = "false")]
    pub fn scan_keys<'p>(
        &self,
        py: Python<'p>,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        limit: u32,
        include_start: bool,
        include_end: bool,
    ) -> PyResult<&'p PyAny> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let range = to_bound_range(start, end, include_start, include_end);
            let keys = inner
                .write()
                .await
                .scan_keys(range, limit)
                .await
                .map_err(to_py_execption)?;
            let py_list = to_py_key_list(keys)?;
            Ok(Python::with_gil(|py| py_list.to_object(py)))
        })
    }

    pub fn lock_keys<'p>(&self, py: Python<'p>, keys: Vec<Vec<u8>>) -> PyResult<&'p PyAny> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner
                .write()
                .await
                .lock_keys(keys)
                .await
                .map_err(to_py_execption)?;
            Ok(Python::with_gil(|py| py.None()))
        })
    }

    pub fn put<'p>(&self, py: Python<'p>, key: Vec<u8>, value: Vec<u8>) -> PyResult<&'p PyAny> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner
                .write()
                .await
                .put(key, value)
                .await
                .map_err(to_py_execption)?;
            Ok(Python::with_gil(|py| py.None()))
        })
    }

    pub fn insert<'p>(&self, py: Python<'p>, key: Vec<u8>, value: Vec<u8>) -> PyResult<&'p PyAny> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner
                .write()
                .await
                .insert(key, value)
                .await
                .map_err(to_py_execption)?;
            Ok(Python::with_gil(|py| py.None()))
        })
    }

    pub fn delete<'p>(&self, py: Python<'p>, key: Vec<u8>) -> PyResult<&'p PyAny> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner
                .write()
                .await
                .delete(key)
                .await
                .map_err(to_py_execption)?;
            Ok(Python::with_gil(|py| py.None()))
        })
    }

    fn commit<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let timestamp = inner
                .write()
                .await
                .commit()
                .await
                .map_err(to_py_execption)?
                .map(|v| v.version());
            Ok(Python::with_gil(|py| timestamp.to_object(py)))
        })
    }

    fn rollback<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner
                .write()
                .await
                .rollback()
                .await
                .map_err(to_py_execption)?;
            Ok(Python::with_gil(|py| py.None()))
        })
    }
}
