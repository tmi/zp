import random
import string
from schema import Small, MediumItem, Medium

def _random_str(length: int = 10) -> str:
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_small_obj() -> Small:
    return Small(
        i1=random.randint(0, 1000),
        i2=random.randint(0, 1000),
        i3=random.randint(0, 1000),
        i4=random.randint(0, 1000),
        i5=random.randint(0, 1000),
        f1=random.random(),
        f2=random.random(),
        f3=random.random(),
        f4=random.random(),
        f5=random.random(),
        s1=_random_str(),
        s2=_random_str(),
        s3=_random_str(),
        s4=_random_str(),
        s5=_random_str(),
    )

def generate_small(n: int) -> list[Small]:
    return [generate_small_obj() for _ in range(n)]

def generate_medium_obj(k: int, d1: int, d2: int, d3: int) -> Medium:
    items = [
        MediumItem(
            i1=random.randint(0, 1000),
            i2=random.randint(0, 1000),
            i3=random.randint(0, 1000),
            f1=random.random(),
            f2=random.random(),
            f3=random.random(),
            s1=_random_str(),
            s2=_random_str(),
            s3=_random_str(),
        )
        for _ in range(k)
    ]

    # Ensure unique keys for dictionaries
    dict1 = {}
    while len(dict1) < d1:
        dict1[random.randint(0, 1000000)] = generate_small_obj()

    dict2 = {}
    while len(dict2) < d2:
        key = tuple(random.randint(0, 100) for _ in range(3))
        dict2[key] = generate_small_obj()

    dict3 = {}
    while len(dict3) < d3:
        dict3[_random_str()] = generate_small_obj()

    return Medium(items=items, dict1=dict1, dict2=dict2, dict3=dict3)

def generate_medium(n: int, k: int, d1: int, d2: int, d3: int) -> list[Medium]:
    return [generate_medium_obj(k, d1, d2, d3) for _ in range(n)]
