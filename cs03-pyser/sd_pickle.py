import pickle
from schema import Small, Medium

def ser_small(obj: Small) -> bytes:
    return pickle.dumps(obj)

def des_small(data: bytes) -> Small:
    return pickle.loads(data)

def ser_medium(obj: Medium) -> bytes:
    return pickle.dumps(obj)

def des_medium(data: bytes) -> Medium:
    return pickle.loads(data)
