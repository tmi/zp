from mcp.types import ClientRequest
import json

data = {
    "jsonrpc": "2.0",
    "id": "1",
    "method": "resources/read",
    "params": {"uri": "magicalResource"}
}

try:
    req = ClientRequest.model_validate(data)
    print("Validation successful")
except Exception as e:
    print(f"Validation failed: {e}")
