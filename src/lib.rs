mod pycoroutine;

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
        .map_err(|err| PyException::new_err(format!("{}", err)))?;

        Ok(Client {
            inner: Arc::new(client),
        })
    }

    fn begin(&self) -> PyCoroutine {
        let inner = self.inner.clone();
        PyCoroutine::new(async move {
            let transaction = inner
                .begin()
                .await
                .map_err(|err| PyException::new_err(format!("{}", err)))?;
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
    fn get(&self, key: Py<PyBytes>) -> PyCoroutine {
        let inner = self.inner.clone();
        PyCoroutine::new(async move {
            let key: tikv_client::Key =
                Python::with_gil(|py| key.as_ref(py).as_bytes().to_vec().into());
            let val = inner
                .read()
                .await
                .get(key)
                .await
                .map_err(|err| PyException::new_err(format!("{}", err)))?;
            let val = val
                .map(|val| -> Py<PyBytes> { Python::with_gil(|py| PyBytes::new(py, &val).into()) });
            Ok(val)
        })
    }

    fn put(&self, key: Py<PyBytes>, val: Py<PyBytes>) -> PyCoroutine {
        let inner = self.inner.clone();
        PyCoroutine::new(async move {
            let (key, val): (tikv_client::Key, tikv_client::Value) = Python::with_gil(|py| {
                (
                    key.as_ref(py).as_bytes().to_vec().into(),
                    val.as_ref(py).as_bytes().to_vec().into(),
                )
            });
            inner
                .write()
                .await
                .put(key, val)
                .await
                .map_err(|err| PyException::new_err(format!("{}", err)))?;
            Ok(())
        })
    }
}
