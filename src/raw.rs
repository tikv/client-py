use std::convert::TryInto;
use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::*;

use crate::pycoroutine::{PyCoroutine, RUNTIME};
use crate::utils::*;

#[pyclass]
pub struct RawClient {
    inner: Arc<tikv_client::RawClient>,
}

#[pymethods]
impl RawClient {
    #[new]
    pub fn new(pd_endpoint: String) -> PyResult<Self> {
        let client = RUNTIME
            .block_on(tikv_client::RawClient::new(vec![pd_endpoint]))
            .map_err(to_py_execption)?;
        Ok(RawClient {
            inner: Arc::new(client),
        })
    }

    #[args(cf = "\"default\"")]
    pub fn get(&self, key: Vec<u8>, cf: &str) -> PyCoroutine {
        let inner: PyResult<tikv_client::RawClient> =
            try { self.inner.with_cf(cf.try_into().map_err(to_py_execption)?) };
        PyCoroutine::new(async move {
            let val: Option<Py<PyBytes>> = inner?
                .get(key)
                .await
                .map_err(to_py_execption)?
                .map(to_py_bytes);
            Ok(val)
        })
    }

    #[args(cf = "\"default\"")]
    pub fn batch_get(&self, keys: Vec<Vec<u8>>, cf: &str) -> PyCoroutine {
        let inner: PyResult<tikv_client::RawClient> =
            try { self.inner.with_cf(cf.try_into().map_err(to_py_execption)?) };
        PyCoroutine::new(async move {
            inner?.batch_get(keys).await.map_err(to_py_execption)?;
            Ok(())
        })
    }

    #[args(include_start = "true", include_end = "false", cf = "\"default\"")]
    pub fn scan(
        &self,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        limit: u32,
        include_start: bool,
        include_end: bool,
        cf: &str,
    ) -> PyCoroutine {
        let inner: PyResult<tikv_client::RawClient> =
            try { self.inner.with_cf(cf.try_into().map_err(to_py_execption)?) };
        PyCoroutine::new(async move {
            let range = to_bound_range(start, end, include_start, include_end);
            let kv_pairs = inner?.scan(range, limit).await.map_err(to_py_execption)?;
            Ok(to_py_dict(kv_pairs)?)
        })
    }

    #[args(include_start = "true", include_end = "false", cf = "\"default\"")]
    pub fn scan_keys(
        &self,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        limit: u32,
        include_start: bool,
        include_end: bool,
        cf: &str,
    ) -> PyCoroutine {
        let inner: PyResult<tikv_client::RawClient> =
            try { self.inner.with_cf(cf.try_into().map_err(to_py_execption)?) };
        PyCoroutine::new(async move {
            let range = to_bound_range(start, end, include_start, include_end);
            let keys = inner?
                .scan_keys(range, limit)
                .await
                .map_err(to_py_execption)?;
            Ok(to_py_key_list(keys.into_iter())?)
        })
    }

    #[args(cf = "\"default\"")]
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>, cf: &str) -> PyCoroutine {
        let inner: PyResult<tikv_client::RawClient> =
            try { self.inner.with_cf(cf.try_into().map_err(to_py_execption)?) };
        PyCoroutine::new(async move {
            inner?.put(key, value).await.map_err(to_py_execption)?;
            Ok(())
        })
    }

    #[args(cf = "\"default\"")]
    pub fn batch_put(&self, pairs: Py<PyDict>, cf: &str) -> PyCoroutine {
        let inner: PyResult<tikv_client::RawClient> =
            try { self.inner.with_cf(cf.try_into().map_err(to_py_execption)?) };
        PyCoroutine::new(async move {
            let pairs = from_py_dict(pairs)?;
            inner?.batch_put(pairs).await.map_err(to_py_execption)?;
            Ok(())
        })
    }

    #[args(cf = "\"default\"")]
    pub fn delete(&self, key: Vec<u8>, cf: &str) -> PyCoroutine {
        let inner: PyResult<tikv_client::RawClient> =
            try { self.inner.with_cf(cf.try_into().map_err(to_py_execption)?) };
        PyCoroutine::new(async move {
            inner?.delete(key).await.map_err(to_py_execption)?;
            Ok(())
        })
    }

    #[args(cf = "\"default\"")]
    pub fn batch_delete(&self, keys: Vec<Vec<u8>>, cf: &str) -> PyCoroutine {
        let inner: PyResult<tikv_client::RawClient> =
            try { self.inner.with_cf(cf.try_into().map_err(to_py_execption)?) };
        PyCoroutine::new(async move {
            inner?.batch_delete(keys).await.map_err(to_py_execption)?;
            Ok(())
        })
    }

    #[args(include_start = "true", include_end = "false", cf = "\"default\"")]
    pub fn delete_range(
        &self,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        include_start: bool,
        include_end: bool,
        cf: &str,
    ) -> PyCoroutine {
        let inner: PyResult<tikv_client::RawClient> =
            try { self.inner.with_cf(cf.try_into().map_err(to_py_execption)?) };
        PyCoroutine::new(async move {
            let range = to_bound_range(start, end, include_start, include_end);
            inner?.delete_range(range).await.map_err(to_py_execption)?;
            Ok(())
        })
    }
}
