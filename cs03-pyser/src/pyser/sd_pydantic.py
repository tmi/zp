from pydantic import BaseModel, TypeAdapter
from typing import List, Dict
from .schema import Small, Medium, MediumItem

class MediumPydantic(BaseModel):
    items: List[MediumItem]
    dict1: Dict[int, Small]
    dict2: Dict[str, Small]
    dict3: Dict[str, Small]

small_adapter = TypeAdapter(Small)

def ser_small(obj: Small) -> bytes:
    return small_adapter.dump_json(obj)

def des_small(data: bytes) -> Small:
    return small_adapter.validate_json(data)

def ser_medium(obj: Medium) -> bytes:
    mp = MediumPydantic(
        items=obj.items,
        dict1=obj.dict1,
        dict2={str(k): v for k, v in obj.dict2.items()},
        dict3=obj.dict3
    )
    return mp.model_dump_json().encode('utf-8')

def des_medium(data: bytes) -> Medium:
    mp = MediumPydantic.model_validate_json(data)

    def parse_tuple(s):
        return tuple(int(x.strip()) for x in s.strip('()').split(','))

    dict2 = {parse_tuple(k): v for k, v in mp.dict2.items()}
    return Medium(items=mp.items, dict1=mp.dict1, dict2=dict2, dict3=mp.dict3)
