// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::*;
use tikv_client::TimestampExt as _;
use tikv_client::TransactionOptions;
use tokio::sync::RwLock;

use crate::pycoroutine::PyCoroutine;
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
    pub fn connect(_cls: &PyType, pd_endpoint: String) -> PyCoroutine {
        PyCoroutine::new(async move {
            let inner = tikv_client::TransactionClient::new(vec![pd_endpoint], None)
                .await
                .map_err(to_py_execption)?;
            Ok(TransactionClient {
                inner: Arc::new(inner),
            })
        })
    }

    #[args(pessimistic = "false")]
    pub fn begin(&self, pessimistic: bool) -> PyCoroutine {
        let inner = self.inner.clone();
        PyCoroutine::new(async move {
            let transaction = if pessimistic {
                inner.begin_pessimistic().await.map_err(to_py_execption)?
            } else {
                inner.begin_optimistic().await.map_err(to_py_execption)?
            };
            Ok(Transaction {
                inner: Arc::new(RwLock::new(transaction)),
            })
        })
    }

    pub fn current_timestamp(&self) -> PyCoroutine {
        let inner = self.inner.clone();
        PyCoroutine::new(async move {
            let timestamp = inner.current_timestamp().await.map_err(to_py_execption)?;
            Ok(timestamp.version())
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

    pub fn gc(&self, safepoint: u64) -> PyCoroutine {
        let inner = self.inner.clone();
        PyCoroutine::new(async move {
            let result = inner
                .gc(tikv_client::Timestamp::from_version(safepoint))
                .await
                .map_err(to_py_execption)?;
            Ok(result)
        })
    }
}

#[pyclass]
pub struct Snapshot {
    inner: Arc<RwLock<tikv_client::Snapshot>>,
}

#[pymethods]
impl Snapshot {
    pub fn get(&self, key: Vec<u8>) -> PyCoroutine {
        let inner = self.inner.clone();
        PyCoroutine::new(async move {
            let val = inner
                .write()
                .await
                .get(key)
                .await
                .map_err(to_py_execption)?
                .map(to_py_bytes);
            Ok(val)
        })
    }

    pub fn key_exists(&self, key: Vec<u8>) -> PyCoroutine {
        let inner = self.inner.clone();
        PyCoroutine::new(async move {
            let val = inner
                .write()
                .await
                .key_exists(key)
                .await
                .map_err(to_py_execption)?;
            Ok(val)
        })
    }

    pub fn batch_get(&self, keys: Vec<Vec<u8>>) -> PyCoroutine {
        let inner = self.inner.clone();
        PyCoroutine::new(async move {
            let kv_pairs = inner
                .write()
                .await
                .batch_get(keys)
                .await
                .map_err(to_py_execption)?;
            let py_dict = to_py_kv_list(kv_pairs)?;
            Ok(py_dict)
        })
    }

    #[args(include_start = "true", include_end = "false")]
    pub fn scan(
        &self,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        limit: u32,
        include_start: bool,
        include_end: bool,
    ) -> PyCoroutine {
        let inner = self.inner.clone();
        PyCoroutine::new(async move {
            let range = to_bound_range(start, end, include_start, include_end);
            let kv_pairs = inner
                .write()
                .await
                .scan(range, limit)
                .await
                .map_err(to_py_execption)?;
            let py_dict = to_py_kv_list(kv_pairs)?;
            Ok(py_dict)
        })
    }

    #[args(include_start = "true", include_end = "false")]
    pub fn scan_keys(
        &self,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        limit: u32,
        include_start: bool,
        include_end: bool,
    ) -> PyCoroutine {
        let inner = self.inner.clone();
        PyCoroutine::new(async move {
            let range = to_bound_range(start, end, include_start, include_end);
            let keys = inner
                .write()
                .await
                .scan_keys(range, limit)
                .await
                .map_err(to_py_execption)?;
            Ok(to_py_key_list(keys)?)
        })
    }
}

#[pyclass]
pub struct Transaction {
    inner: Arc<RwLock<tikv_client::Transaction>>,
}

#[pymethods]
impl Transaction {
    pub fn get(&self, key: Vec<u8>) -> PyCoroutine {
        let inner = self.inner.clone();
        PyCoroutine::new(async move {
            let val = inner
                .write()
                .await
                .get(key)
                .await
                .map_err(to_py_execption)?
                .map(to_py_bytes);
            Ok(val)
        })
    }

    pub fn get_for_update(&self, key: Vec<u8>) -> PyCoroutine {
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

    pub fn key_exists(&self, key: Vec<u8>) -> PyCoroutine {
        let inner = self.inner.clone();
        PyCoroutine::new(async move {
            let val = inner
                .write()
                .await
                .key_exists(key)
                .await
                .map_err(to_py_execption)?;
            Ok(val)
        })
    }

    pub fn batch_get(&self, keys: Vec<Vec<u8>>) -> PyCoroutine {
        let inner = self.inner.clone();
        PyCoroutine::new(async move {
            let kv_pairs = inner
                .write()
                .await
                .batch_get(keys)
                .await
                .map_err(to_py_execption)?;
            Ok(to_py_kv_list(kv_pairs)?)
        })
    }

    pub fn batch_get_for_update(&self, keys: Vec<Vec<u8>>) -> PyCoroutine {
        let inner = self.inner.clone();
        PyCoroutine::new(async move {
            let kv_pairs = inner
                .write()
                .await
                .batch_get_for_update(keys)
                .await
                .map_err(to_py_execption)?;
            Ok(to_py_kv_list(kv_pairs)?)
        })
    }

    #[args(include_start = "true", include_end = "false")]
    pub fn scan(
        &self,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        limit: u32,
        include_start: bool,
        include_end: bool,
    ) -> PyCoroutine {
        let inner = self.inner.clone();
        PyCoroutine::new(async move {
            let range = to_bound_range(start, end, include_start, include_end);
            let kv_pairs = inner
                .write()
                .await
                .scan(range, limit)
                .await
                .map_err(to_py_execption)?;
            Ok(to_py_kv_list(kv_pairs)?)
        })
    }

    #[args(include_start = "true", include_end = "false")]
    pub fn scan_keys(
        &self,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        limit: u32,
        include_start: bool,
        include_end: bool,
    ) -> PyCoroutine {
        let inner = self.inner.clone();
        PyCoroutine::new(async move {
            let range = to_bound_range(start, end, include_start, include_end);
            let keys = inner
                .write()
                .await
                .scan_keys(range, limit)
                .await
                .map_err(to_py_execption)?;
            Ok(to_py_key_list(keys)?)
        })
    }

    pub fn lock_keys(&self, keys: Vec<Vec<u8>>) -> PyCoroutine {
        let inner = self.inner.clone();
        PyCoroutine::new(async move {
            inner
                .write()
                .await
                .lock_keys(keys)
                .await
                .map_err(to_py_execption)?;
            Ok(())
        })
    }

    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> PyCoroutine {
        let inner = self.inner.clone();
        PyCoroutine::new(async move {
            inner
                .write()
                .await
                .put(key, value)
                .await
                .map_err(to_py_execption)?;
            Ok(())
        })
    }

    pub fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> PyCoroutine {
        let inner = self.inner.clone();
        PyCoroutine::new(async move {
            inner
                .write()
                .await
                .insert(key, value)
                .await
                .map_err(to_py_execption)?;
            Ok(())
        })
    }

    pub fn delete(&self, key: Vec<u8>) -> PyCoroutine {
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
        PyCoroutine::new(async move {
            let timestamp = inner
                .write()
                .await
                .commit()
                .await
                .map_err(to_py_execption)?
                .map(|v| v.version());
            Ok(timestamp)
        })
    }
}
