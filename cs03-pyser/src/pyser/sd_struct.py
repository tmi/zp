import struct
from .schema import Small, Medium, MediumItem
import io

def _ser_str(s: str) -> bytes:
    b = s.encode('utf-8')
    return struct.pack('Q', len(b)) + b

def _des_str(f: io.BytesIO) -> str:
    len_bytes = f.read(8)
    if not len_bytes:
        return ""
    length = struct.unpack('Q', len_bytes)[0]
    return f.read(length).decode('utf-8')

def ser_small(obj: Small) -> bytes:
    res = struct.pack('5q5d', obj.i1, obj.i2, obj.i3, obj.i4, obj.i5,
                             obj.f1, obj.f2, obj.f3, obj.f4, obj.f5)
    res += _ser_str(obj.s1)
    res += _ser_str(obj.s2)
    res += _ser_str(obj.s3)
    res += _ser_str(obj.s4)
    res += _ser_str(obj.s5)
    return res

def des_small_from_file(f: io.BytesIO) -> Small:
    header = f.read(struct.calcsize('5q5d'))
    if not header:
         raise ValueError("Empty data")
    vals = struct.unpack('5q5d', header)
    s1 = _des_str(f)
    s2 = _des_str(f)
    s3 = _des_str(f)
    s4 = _des_str(f)
    s5 = _des_str(f)
    v = vals
    return Small(v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7], v[8], v[9], s1, s2, s3, s4, s5)

def des_small(data: bytes) -> Small:
    f = io.BytesIO(data)
    return des_small_from_file(f)

def _ser_medium_item(obj: MediumItem) -> bytes:
    res = struct.pack('3q3d', obj.i1, obj.i2, obj.i3, obj.f1, obj.f2, obj.f3)
    res += _ser_str(obj.s1)
    res += _ser_str(obj.s2)
    res += _ser_str(obj.s3)
    return res

def _des_medium_item(f: io.BytesIO) -> MediumItem:
    header = f.read(struct.calcsize('3q3d'))
    vals = struct.unpack('3q3d', header)
    s1 = _des_str(f)
    s2 = _des_str(f)
    s3 = _des_str(f)
    v = vals
    return MediumItem(v[0], v[1], v[2], v[3], v[4], v[5], s1, s2, s3)

def ser_medium(obj: Medium) -> bytes:
    res = struct.pack('Q', len(obj.items))
    for item in obj.items:
        res += _ser_medium_item(item)

    res += struct.pack('Q', len(obj.dict1))
    for k, v in obj.dict1.items():
        res += struct.pack('q', k)
        res += ser_small(v)

    res += struct.pack('Q', len(obj.dict2))
    for k, v in obj.dict2.items():
        res += struct.pack('Q', len(k))
        res += struct.pack(f'{len(k)}q', *k)
        res += ser_small(v)

    res += struct.pack('Q', len(obj.dict3))
    for k, v in obj.dict3.items():
        res += _ser_str(k)
        res += ser_small(v)
    return res

def des_medium(data: bytes) -> Medium:
    f = io.BytesIO(data)
    len_bytes = f.read(8)
    if not len_bytes:
        raise ValueError("Empty data")
    num_items = struct.unpack('Q', len_bytes)[0]
    items = [_des_medium_item(f) for _ in range(num_items)]

    num_dict1 = struct.unpack('Q', f.read(8))[0]
    dict1 = {}
    for _ in range(num_dict1):
        k = struct.unpack('q', f.read(8))[0]
        v = des_small_from_file(f)
        dict1[k] = v

    num_dict2 = struct.unpack('Q', f.read(8))[0]
    dict2 = {}
    for _ in range(num_dict2):
        t_len_bytes = f.read(8)
        if not t_len_bytes:
            break
        t_len = struct.unpack('Q', t_len_bytes)[0]
        k = struct.unpack(f'{t_len}q', f.read(8 * t_len))
        v = des_small_from_file(f)
        dict2[k] = v

    num_dict3_bytes = f.read(8)
    if num_dict3_bytes:
        num_dict3 = struct.unpack('Q', num_dict3_bytes)[0]
        dict3 = {}
        for _ in range(num_dict3):
            k = _des_str(f)
            v = des_small_from_file(f)
            dict3[k] = v
    else:
        dict3 = {}

    return Medium(items=items, dict1=dict1, dict2=dict2, dict3=dict3)
