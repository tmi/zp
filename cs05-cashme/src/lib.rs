use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use cashme_core as core;
use core::Message;

#[pyclass]
#[derive(Clone)]
pub struct Request {
    pub inner: core::Request,
}

#[pymethods]
impl Request {
    #[new]
    #[pyo3(signature = (size))]
    pub fn new(size: u32) -> Self {
        Self { inner: core::Request::new(size) }
    }

    #[getter]
    pub fn size(&self) -> u32 {
        self.inner.size
    }

    #[setter]
    pub fn set_size(&mut self, size: u32) {
        self.inner.size = size;
    }
}

#[pyclass]
#[derive(Clone)]
pub struct Response {
    pub inner: core::Response,
}

#[pymethods]
impl Response {
    #[new]
    #[pyo3(signature = (error=None, id=None))]
    pub fn new(error: Option<String>, id: Option<String>) -> Self {
        Self { inner: core::Response::new(error, id) }
    }

    #[getter]
    pub fn error(&self) -> Option<String> {
        self.inner.error.clone()
    }

    #[setter]
    pub fn set_error(&mut self, error: Option<String>) {
        self.inner.error = error;
    }

    #[getter]
    pub fn id(&self) -> Option<String> {
        self.inner.id.clone()
    }

    #[setter]
    pub fn set_id(&mut self, id: Option<String>) {
        self.inner.id = id;
    }
}

#[pyfunction]
pub fn new_request(size: u32) -> Request {
    Request { inner: core::Request::new(size) }
}

#[pyfunction]
pub fn serialize_message(py: Python<'_>, message: PyObject) -> PyResult<Vec<u8>> {
    if let Ok(req) = message.extract::<Py<Request>>(py) {
        let req = req.borrow(py);
        Ok(req.inner.serialize_message())
    } else if let Ok(res) = message.extract::<Py<Response>>(py) {
        let res = res.borrow(py);
        Ok(res.inner.serialize_message())
    } else {
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>("Expected Request or Response"))
    }
}

#[pyfunction]
pub fn parse_message(py: Python<'_>, bytes: &[u8]) -> PyResult<PyObject> {
    match core::deserialize_internal(bytes) {
        Ok(core::MessageUnion::Request(req)) => {
            let py_req = Request { inner: req };
            Ok(py_req.into_py_any(py)?)
        }
        Ok(core::MessageUnion::Response(res)) => {
            let py_res = Response { inner: res };
            Ok(py_res.into_py_any(py)?)
        }
        Err(e) => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(e)),
    }
}

#[pymodule]
fn _cashme(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Request>()?;
    m.add_class::<Response>()?;
    m.add_function(wrap_pyfunction!(new_request, m)?)?;
    m.add_function(wrap_pyfunction!(serialize_message, m)?)?;
    m.add_function(wrap_pyfunction!(parse_message, m)?)?;
    Ok(())
}
