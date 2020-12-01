// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::future::Future;
use std::pin::Pin;

use futures::prelude::*;
use lazy_static::lazy_static;
use pyo3::class::{iter::IterNextOutput, *};
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use tokio::runtime::Runtime;
use tokio::sync::oneshot::{self, error::TryRecvError};

lazy_static! {
    pub(crate) static ref RUNTIME: Runtime = Runtime::new().unwrap();
}

#[pyclass]
pub struct PyCoroutine {
    task: Option<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>,
    rx: oneshot::Receiver<PyResult<Py<PyAny>>>,
}

impl PyCoroutine {
    pub fn new<T: IntoPy<Py<PyAny>>>(
        task: impl Future<Output = PyResult<T>> + Send + 'static,
    ) -> Self {
        let (tx, rx) = oneshot::channel();

        let task = async move {
            let val = task.await;
            let pyval = val.map(|val| Python::with_gil(|py| val.into_py(py)));
            tx.send(pyval).ok();
        }
        .boxed();

        PyCoroutine {
            task: Some(task),
            rx,
        }
    }
}

#[pyproto]
impl PyAsyncProtocol for PyCoroutine {
    fn __await__(mut slf: PyRefMut<Self>) -> PyRefMut<Self> {
        if let Some(task) = Option::take(&mut slf.task) {
            RUNTIME.spawn(task);
        } else {
            Python::with_gil(|py| {
                PyException::new_err("Awaited twice on PyCoroutine.").restore(py)
            });
        }
        slf
    }
}

#[pyproto]
impl PyIterProtocol for PyCoroutine {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<Self>) -> IterNextOutput<Option<()>, Py<PyAny>> {
        match slf.rx.try_recv() {
            Ok(Ok(val)) => IterNextOutput::Return(val),
            Ok(Err(err)) => {
                Python::with_gil(|py| err.restore(py));
                IterNextOutput::Yield(None)
            }
            Err(TryRecvError::Empty) => IterNextOutput::Yield(None),
            Err(TryRecvError::Closed) => panic!("oneshot channel closed"),
        }
    }
}
