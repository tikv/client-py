// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::TryInto;
use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::*;
use pyo3::ToPyObject;
use pyo3_asyncio::tokio::future_into_py;

use crate::utils::*;

#[pyclass]
pub struct RawClient {
    inner: Arc<tikv_client::RawClient>,
}

#[pymethods]
impl RawClient {
    #[classmethod]
    pub fn connect<'p>(
        _cls: &PyType,
        py: Python<'p>,
        pd_endpoints: Vec<String>,
    ) -> PyResult<&'p PyAny> {
        future_into_py(py, async move {
            let inner = tikv_client::RawClient::new(pd_endpoints, None)
                .await
                .map_err(to_py_execption)?;
            let client = RawClient {
                inner: Arc::new(inner),
            };
            Python::with_gil(|py| PyCell::new(py, client).map(|py_cell| py_cell.to_object(py)))
        })
    }

    pub fn get<'p>(&self, py: Python<'p>, key: Vec<u8>, cf: &str) -> PyResult<&'p PyAny> {
        let inner: PyResult<tikv_client::RawClient> =
            try { self.inner.with_cf(cf.try_into().map_err(to_py_execption)?) };
        future_into_py(py, async move {
            let val: Option<Py<PyBytes>> = inner?
                .get(key)
                .await
                .map_err(to_py_execption)?
                .map(to_py_bytes);
            Ok(Python::with_gil(|py| val.to_object(py)))
        })
    }

    pub fn batch_get<'p>(
        &self,
        py: Python<'p>,
        keys: Vec<Vec<u8>>,
        cf: &str,
    ) -> PyResult<&'p PyAny> {
        let inner: PyResult<tikv_client::RawClient> =
            try { self.inner.with_cf(cf.try_into().map_err(to_py_execption)?) };
        future_into_py(py, async move {
            let kvpairs = inner?.batch_get(keys).await.map_err(to_py_execption)?;
            let py_list = to_py_kv_list(kvpairs)?;
            Ok(Python::with_gil(|py| py_list.to_object(py)))
        })
    }

    pub fn get_key_ttl_secs<'p>(
        &self,
        py: Python<'p>,
        key: Vec<u8>,
        cf: &str,
    ) -> PyResult<&'p PyAny> {
        let inner: PyResult<tikv_client::RawClient> =
            try { self.inner.with_cf(cf.try_into().map_err(to_py_execption)?) };
        future_into_py(py, async move {
            let ttl: Option<Py<PyAny>> = inner?
                .get_key_ttl_secs(key)
                .await
                .map_err(to_py_execption)?
                .map(to_py_int);
            Ok(Python::with_gil(|py| ttl.to_object(py)))
        })
    }

    pub fn scan<'p>(
        &self,
        py: Python<'p>,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        limit: u32,
        include_start: bool,
        include_end: bool,
        cf: &str,
    ) -> PyResult<&'p PyAny> {
        let inner: PyResult<tikv_client::RawClient> =
            try { self.inner.with_cf(cf.try_into().map_err(to_py_execption)?) };
        future_into_py(py, async move {
            let range = to_bound_range(start, end, include_start, include_end);
            let kvpairs = inner?.scan(range, limit).await.map_err(to_py_execption)?;
            let py_list = to_py_kv_list(kvpairs)?;
            Ok(Python::with_gil(|py| py_list.to_object(py)))
        })
    }

    pub fn scan_keys<'p>(
        &self,
        py: Python<'p>,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        limit: u32,
        include_start: bool,
        include_end: bool,
        cf: &str,
    ) -> PyResult<&'p PyAny> {
        let inner: PyResult<tikv_client::RawClient> =
            try { self.inner.with_cf(cf.try_into().map_err(to_py_execption)?) };
        future_into_py(py, async move {
            let range = to_bound_range(start, end, include_start, include_end);
            let keys = inner?
                .scan_keys(range, limit)
                .await
                .map_err(to_py_execption)?;
            let py_list = to_py_key_list(keys)?;
            Ok(Python::with_gil(|py| py_list.to_object(py)))
        })
    }

    pub fn put<'p>(
        &self,
        py: Python<'p>,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_secs: u64,
        cf: &str,
    ) -> PyResult<&'p PyAny> {
        let inner: PyResult<tikv_client::RawClient> =
            try { self.inner.with_cf(cf.try_into().map_err(to_py_execption)?) };
        future_into_py(py, async move {
            inner?
                .put_with_ttl(key, value, ttl_secs)
                .await
                .map_err(to_py_execption)?;
            Ok(Python::with_gil(|py| py.None()))
        })
    }

    pub fn batch_put<'p>(
        &self,
        py: Python<'p>,
        pairs: Py<PyDict>,
        cf: &str,
    ) -> PyResult<&'p PyAny> {
        let inner: PyResult<tikv_client::RawClient> =
            try { self.inner.with_cf(cf.try_into().map_err(to_py_execption)?) };
        future_into_py(py, async move {
            let pairs = from_py_dict(pairs)?;
            inner?.batch_put(pairs).await.map_err(to_py_execption)?;
            Ok(Python::with_gil(|py| py.None()))
        })
    }

    pub fn batch_put_with_ttl<'p>(
        &self,
        py: Python<'p>,
        pairs_with_ttls_secs: Py<PyDict>,
        cf: &str,
    ) -> PyResult<&'p PyAny> {
        let inner: PyResult<tikv_client::RawClient> =
            try { self.inner.with_cf(cf.try_into().map_err(to_py_execption)?) };
        future_into_py(py, async move {
            let pairs_with_ttls_secs = from_py_dict_with_ttl(pairs_with_ttls_secs)?;
            inner?
                .batch_put_with_ttl(pairs_with_ttls_secs)
                .await
                .map_err(to_py_execption)?;
            Ok(Python::with_gil(|py| py.None()))
        })
    }

    pub fn delete<'p>(&self, py: Python<'p>, key: Vec<u8>, cf: &str) -> PyResult<&'p PyAny> {
        let inner: PyResult<tikv_client::RawClient> =
            try { self.inner.with_cf(cf.try_into().map_err(to_py_execption)?) };
        future_into_py(py, async move {
            inner?.delete(key).await.map_err(to_py_execption)?;
            Ok(Python::with_gil(|py| py.None()))
        })
    }

    pub fn batch_delete<'p>(
        &self,
        py: Python<'p>,
        keys: Vec<Vec<u8>>,
        cf: &str,
    ) -> PyResult<&'p PyAny> {
        let inner: PyResult<tikv_client::RawClient> =
            try { self.inner.with_cf(cf.try_into().map_err(to_py_execption)?) };
        future_into_py(py, async move {
            inner?.batch_delete(keys).await.map_err(to_py_execption)?;
            Ok(Python::with_gil(|py| py.None()))
        })
    }

    pub fn delete_range<'p>(
        &self,
        py: Python<'p>,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        include_start: bool,
        include_end: bool,
        cf: &str,
    ) -> PyResult<&'p PyAny> {
        let inner: PyResult<tikv_client::RawClient> =
            try { self.inner.with_cf(cf.try_into().map_err(to_py_execption)?) };
        future_into_py(py, async move {
            let range = to_bound_range(start, end, include_start, include_end);
            inner?.delete_range(range).await.map_err(to_py_execption)?;
            Ok(Python::with_gil(|py| py.None()))
        })
    }
}
