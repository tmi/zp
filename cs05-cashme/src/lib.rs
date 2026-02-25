use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

#[pyclass]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Request {
    #[pyo3(get, set)]
    pub size: u32,
}

#[pymethods]
impl Request {
    #[new]
    #[pyo3(signature = (size))]
    pub fn new(size: u32) -> Self {
        Request { size }
    }
}

#[pyclass]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Response {
    #[pyo3(get, set)]
    pub error: Option<String>,
    #[pyo3(get, set)]
    pub id: Option<String>,
}

#[pymethods]
impl Response {
    #[new]
    #[pyo3(signature = (error=None, id=None))]
    pub fn new(error: Option<String>, id: Option<String>) -> Self {
        Response { error, id }
    }
}

pub trait Message {
    fn serialize_message(&self) -> Vec<u8>;
}

impl Message for Request {
    fn serialize_message(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }
}

impl Message for Response {
    fn serialize_message(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }
}

pub enum MessageUnion {
    Request(Request),
    Response(Response),
}

pub fn deserialize_internal(bytes: &[u8]) -> Result<MessageUnion, String> {
    // Try parsing as Request
    if let Ok(req) = serde_json::from_slice::<Request>(bytes) {
        // Simple heuristic: Request must have 'size'.
        // Actually serde_json::from_slice will fail if 'size' is missing.
        return Ok(MessageUnion::Request(req));
    }
    // Try parsing as Response
    if let Ok(res) = serde_json::from_slice::<Response>(bytes) {
        return Ok(MessageUnion::Response(res));
    }
    Err("Failed to deserialize message".to_string())
}

#[pyfunction]
pub fn new_request(size: u32) -> Request {
    Request { size }
}

#[pyfunction]
pub fn serialize_message(py: Python, message: PyObject) -> PyResult<Vec<u8>> {
    if let Ok(req) = message.extract::<Request>(py) {
        Ok(req.serialize_message())
    } else if let Ok(res) = message.extract::<Response>(py) {
        Ok(res.serialize_message())
    } else {
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>("Expected Request or Response"))
    }
}

#[pyfunction]
pub fn parse_message(bytes: &[u8]) -> PyResult<PyObject> {
    Python::with_gil(|py| {
        match deserialize_internal(bytes) {
            Ok(MessageUnion::Request(req)) => Ok(req.into_py(py)),
            Ok(MessageUnion::Response(res)) => Ok(res.into_py(py)),
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(e)),
        }
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialization() {
        let req = Request { size: 42 };
        let bytes = req.serialize_message();
        match deserialize_internal(&bytes).unwrap() {
            MessageUnion::Request(r) => assert_eq!(r.size, 42),
            _ => panic!("Expected Request"),
        }

        let res = Response {
            error: Some("none".to_string()),
            id: Some("123".to_string()),
        };
        let bytes = res.serialize_message();
        match deserialize_internal(&bytes).unwrap() {
            MessageUnion::Response(r) => {
                assert_eq!(r.error, Some("none".to_string()));
                assert_eq!(r.id, Some("123".to_string()));
            }
            _ => panic!("Expected Response"),
        }
    }
}
