from dataclasses import dataclass
from typing import List, Dict, Tuple

@dataclass(frozen=True)
class Small:
    i1: int
    i2: int
    i3: int
    i4: int
    i5: int
    f1: float
    f2: float
    f3: float
    f4: float
    f5: float
    s1: str
    s2: str
    s3: str
    s4: str
    s5: str

@dataclass(frozen=True)
class MediumItem:
    i1: int
    i2: int
    i3: int
    f1: float
    f2: float
    f3: float
    s1: str
    s2: str
    s3: str

@dataclass(frozen=True)
class Medium:
    items: List[MediumItem]
    dict1: Dict[int, Small]
    dict2: Dict[Tuple[int, ...], Small]
    dict3: Dict[str, Small]
