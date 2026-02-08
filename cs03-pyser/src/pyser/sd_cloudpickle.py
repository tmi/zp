import cloudpickle
from .schema import Small, Medium

def ser_small(obj: Small) -> bytes:
    return cloudpickle.dumps(obj)

def des_small(data: bytes) -> Small:
    return cloudpickle.loads(data)

def ser_medium(obj: Medium) -> bytes:
    return cloudpickle.dumps(obj)

def des_medium(data: bytes) -> Medium:
    return cloudpickle.loads(data)
