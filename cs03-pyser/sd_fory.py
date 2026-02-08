import pyfory
from schema import Small, Medium, MediumItem
from dataclasses import dataclass
from typing import List, Dict

@dataclass
class MediumShadow:
    items: List[MediumItem]
    dict1: Dict[int, Small]
    dict2: Dict[str, Small]
    dict3: Dict[str, Small]

# Create Fory instance and register types
fory = pyfory.ThreadSafeFory()
fory.register_type(Small)
fory.register_type(MediumItem)
fory.register_type(MediumShadow)

def ser_small(obj: Small) -> bytes:
    return fory.serialize(obj)

def des_small(data: bytes) -> Small:
    return fory.deserialize(data)

def ser_medium(obj: Medium) -> bytes:
    shadow = MediumShadow(
        items=obj.items,
        dict1=obj.dict1,
        dict2={str(k): v for k, v in obj.dict2.items()},
        dict3=obj.dict3
    )
    return fory.serialize(shadow)

def des_medium(data: bytes) -> Medium:
    shadow = fory.deserialize(data)

    def parse_tuple(s):
        return tuple(int(x.strip()) for x in s.strip('()').split(','))

    dict2 = {parse_tuple(k): v for k, v in shadow.dict2.items()}
    return Medium(items=shadow.items, dict1=shadow.dict1, dict2=dict2, dict3=shadow.dict3)
