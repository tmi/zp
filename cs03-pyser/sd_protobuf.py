import schema_pb2 as _schema_pb2
from schema import Small, Medium, MediumItem
from typing import Any, cast

schema_pb2: Any = _schema_pb2

def _small_to_proto(v: Small) -> Any:
    return schema_pb2.SmallProto(
        i1=v.i1, i2=v.i2, i3=v.i3, i4=v.i4, i5=v.i5,
        f1=v.f1, f2=v.f2, f3=v.f3, f4=v.f4, f5=v.f5,
        s1=v.s1, s2=v.s2, s3=v.s3, s4=v.s4, s5=v.s5
    )

def _proto_to_small(sp: Any) -> Small:
    return Small(
        i1=sp.i1, i2=sp.i2, i3=sp.i3, i4=sp.i4, i5=sp.i5,
        f1=sp.f1, f2=sp.f2, f3=sp.f3, f4=sp.f4, f5=sp.f5,
        s1=sp.s1, s2=sp.s2, s3=sp.s3, s4=sp.s4, s5=sp.s5
    )

def ser_small(obj: Small) -> bytes:
    p = _small_to_proto(obj)
    return cast(bytes, p.SerializeToString())

def des_small(data: bytes) -> Small:
    p = schema_pb2.SmallProto()
    p.ParseFromString(data)
    return _proto_to_small(p)

def ser_medium(obj: Medium) -> bytes:
    p = schema_pb2.MediumProto()
    for item in obj.items:
        p.items.add(
            i1=item.i1, i2=item.i2, i3=item.i3,
            f1=item.f1, f2=item.f2, f3=item.f3,
            s1=item.s1, s2=item.s2, s3=item.s3
        )
    for k, v in obj.dict1.items():
        p.dict1[k].CopyFrom(_small_to_proto(v))
    for k, v in obj.dict2.items():
        entry = p.dict2.add()
        entry.key.values.extend(k)
        entry.value.CopyFrom(_small_to_proto(v))
    for k, v in obj.dict3.items():
        p.dict3[k].CopyFrom(_small_to_proto(v))
    return cast(bytes, p.SerializeToString())

def des_medium(data: bytes) -> Medium:
    p = schema_pb2.MediumProto()
    p.ParseFromString(data)

    items = [
        MediumItem(
            i1=item.i1, i2=item.i2, i3=item.i3,
            f1=item.f1, f2=item.f2, f3=item.f3,
            s1=item.s1, s2=item.s2, s3=item.s3
        )
        for item in p.items
    ]

    dict1 = {k: _proto_to_small(v) for k, v in p.dict1.items()}
    dict2 = {tuple(entry.key.values): _proto_to_small(entry.value) for entry in p.dict2}
    dict3 = {k: _proto_to_small(v) for k, v in p.dict3.items()}

    return Medium(items=items, dict1=dict1, dict2=dict2, dict3=dict3)
