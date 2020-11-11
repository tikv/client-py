use std::convert::TryInto;
use std::ops::Bound;
use std::sync::Arc;

use futures::executor::block_on;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::*;
use tikv_client::TimestampExt as _;
use tokio::sync::RwLock;

use crate::pycoroutine::PyCoroutine;
use crate::utils::*;

#[pyclass]
pub struct Client {
    inner: Arc<tikv_client::RawClient>,
}

#[pymethods]
impl Client {
    #[new]
    pub fn new(pd_endpoint: String) -> PyResult<Self> {
        let client = block_on(tikv_client::RawClient::new(tikv_client::Config::new(vec![
            pd_endpoint,
        ])))
        .map_err(to_py_execption)?;
        Ok(Client {
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
             inner?
                .delete(key)
                .await
                .map_err(to_py_execption)?;
            Ok(())
        })
    }

    
}
