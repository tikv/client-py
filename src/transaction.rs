use std::convert::TryInto;
use std::ops::Bound;
use std::sync::Arc;

use futures::executor::block_on;
use pyo3::prelude::*;
use pyo3::types::*;
use tikv_client::TimestampExt as _;
use tokio::sync::RwLock;

use crate::pycoroutine::PyCoroutine;
use crate::utils::*;

#[pyclass]
pub struct Client {
    inner: Arc<tikv_client::TransactionClient>,
}

#[pymethods]
impl Client {
    #[new]
    pub fn new(pd_endpoint: String) -> PyResult<Self> {
        let client = block_on(tikv_client::TransactionClient::new(
            tikv_client::Config::new(vec![pd_endpoint]),
        ))
        .map_err(to_py_execption)?;
        Ok(Client {
            inner: Arc::new(client),
        })
    }

    #[args(pessimistic = "false")]
    pub fn begin(&self, pessimistic: bool) -> PyCoroutine {
        let inner = self.inner.clone();
        PyCoroutine::new(async move {
            let transaction = if pessimistic {
                inner.begin_pessimistic().await.map_err(to_py_execption)?
            } else {
                inner.begin().await.map_err(to_py_execption)?
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

    pub fn snapshot(&self, timestamp: u64) -> Snapshot {
        Snapshot {
            inner: Arc::new(
                self.inner
                    .snapshot(tikv_client::Timestamp::from_version(timestamp)),
            ),
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
    inner: Arc<tikv_client::Snapshot>,
}

#[pymethods]
impl Snapshot {
    pub fn get(&self, key: Vec<u8>) -> PyCoroutine {
        let inner = self.inner.clone();
        PyCoroutine::new(async move {
            let val = inner
                .get(key)
                .await
                .map_err(to_py_execption)?
                .map(to_py_bytes);
            Ok(val)
        })
    }

    pub fn batch_get(&self, keys: Vec<Vec<u8>>) -> PyCoroutine {
        let inner = self.inner.clone();
        PyCoroutine::new(async move {
            let kv_pairs = inner.batch_get(keys).await.map_err(to_py_execption)?;
            let py_dict = to_py_dict(kv_pairs)?;
            Ok(py_dict)
        })
    }

    #[args(
        limit = 1,
        include_start = "true",
        include_end = "false",
        key_only = "false"
    )]
    pub fn scan(
        &self,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        limit: u32,
        include_start: bool,
        include_end: bool,
        key_only: bool,
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
                .scan(range, limit, key_only)
                .await
                .map_err(to_py_execption)?;
            let py_dict = to_py_dict(kv_pairs)?;
            Ok(py_dict)
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
                .read()
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

    pub fn batch_get(&self, keys: Vec<Vec<u8>>) -> PyCoroutine {
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

    pub fn batch_get_for_update(&self, keys: Vec<Vec<u8>>) -> PyCoroutine {
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
        key_only = "false"
    )]
    pub fn scan(
        &self,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        limit: u32,
        include_start: bool,
        include_end: bool,
        key_only: bool,
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
                .scan(range, limit, key_only)
                .await
                .map_err(to_py_execption)?;
            let py_dict = to_py_dict(kv_pairs)?;
            Ok(py_dict)
        })
    }

    pub fn lock_keys(&self, keys: Vec<Vec<u8>>) -> PyCoroutine {
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
        PyCoroutine::new(async move { inner.write().await.commit().await.map_err(to_py_execption) })
    }
}
