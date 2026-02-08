import orjson
from schema import Small, Medium, MediumItem

def ser_small(obj: Small) -> bytes:
    return orjson.dumps(obj, option=orjson.OPT_SERIALIZE_DATACLASS)

def des_small(data: bytes) -> Small:
    d = orjson.loads(data)
    return Small(**d)

def ser_medium(obj: Medium) -> bytes:
    # orjson handles dataclasses with OPT_SERIALIZE_DATACLASS.
    # JSON keys MUST be strings.
    d = {
        "items": obj.items,
        "dict1": {str(k): v for k, v in obj.dict1.items()},
        "dict2": {str(k): v for k, v in obj.dict2.items()},
        "dict3": obj.dict3
    }
    return orjson.dumps(d, option=orjson.OPT_SERIALIZE_DATACLASS)

def des_medium(data: bytes) -> Medium:
    d = orjson.loads(data)
    items = [MediumItem(**item) for item in d['items']]
    dict1 = {int(k): Small(**v) for k, v in d['dict1'].items()}

    def parse_tuple(s):
        # s is something like "(1, 2, 3)"
        return tuple(int(x.strip()) for x in s.strip('()').split(','))

    dict2 = {parse_tuple(k): Small(**v) for k, v in d['dict2'].items()}
    dict3 = {k: Small(**v) for k, v in d['dict3'].items()}
    return Medium(items=items, dict1=dict1, dict2=dict2, dict3=dict3)
