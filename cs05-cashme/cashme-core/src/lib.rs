use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Request {
    pub size: u32,
}

impl Request {
    pub fn new(size: u32) -> Self {
        Request { size }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Response {
    pub error: Option<String>,
    pub id: Option<String>,
}

impl Response {
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
        return Ok(MessageUnion::Request(req));
    }
    // Try parsing as Response
    if let Ok(res) = serde_json::from_slice::<Response>(bytes) {
        return Ok(MessageUnion::Response(res));
    }
    Err("Failed to deserialize message".to_string())
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
