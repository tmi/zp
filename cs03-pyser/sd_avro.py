import avro.schema
import avro.io
import io
from schema import Small, Medium, MediumItem
from typing import Any, cast

SMALL_SCHEMA_STR = """
{
  "type": "record",
  "name": "Small",
  "fields": [
    {"name": "i1", "type": "long"},
    {"name": "i2", "type": "long"},
    {"name": "i3", "type": "long"},
    {"name": "i4", "type": "long"},
    {"name": "i5", "type": "long"},
    {"name": "f1", "type": "double"},
    {"name": "f2", "type": "double"},
    {"name": "f3", "type": "double"},
    {"name": "f4", "type": "double"},
    {"name": "f5", "type": "double"},
    {"name": "s1", "type": "string"},
    {"name": "s2", "type": "string"},
    {"name": "s3", "type": "string"},
    {"name": "s4", "type": "string"},
    {"name": "s5", "type": "string"}
  ]
}
"""

MEDIUM_SCHEMA_STR = """
{
  "type": "record",
  "name": "Medium",
  "fields": [
    {
      "name": "items",
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "MediumItem",
          "fields": [
            {"name": "i1", "type": "long"},
            {"name": "i2", "type": "long"},
            {"name": "i3", "type": "long"},
            {"name": "f1", "type": "double"},
            {"name": "f2", "type": "double"},
            {"name": "f3", "type": "double"},
            {"name": "s1", "type": "string"},
            {"name": "s2", "type": "string"},
            {"name": "s3", "type": "string"}
          ]
        }
      }
    },
    {
      "name": "dict1",
      "type": {"type": "map", "values": "Small"}
    },
    {
      "name": "dict2",
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "Dict2Entry",
          "fields": [
            {"name": "key", "type": {"type": "array", "items": "long"}},
            {"name": "value", "type": "Small"}
          ]
        }
      }
    },
    {
      "name": "dict3",
      "type": {"type": "map", "values": "Small"}
    }
  ]
}
"""

COMBINED_SCHEMA_STR = f"[{SMALL_SCHEMA_STR}, {MEDIUM_SCHEMA_STR}]"
# Use Any to avoid ty errors on internal avro schema structure
SCHEMA: Any = avro.schema.parse(COMBINED_SCHEMA_STR)
SMALL_SCHEMA = cast(Any, SCHEMA).schemas[0]
MEDIUM_SCHEMA = cast(Any, SCHEMA).schemas[1]

def ser_small(obj: Small) -> bytes:
    writer = avro.io.DatumWriter(SMALL_SCHEMA)
    bytes_io = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_io)
    writer.write(obj.__dict__, encoder)
    return bytes_io.getvalue()

def des_small(data: bytes) -> Small:
    reader = avro.io.DatumReader(SMALL_SCHEMA)
    bytes_io = io.BytesIO(data)
    decoder = avro.io.BinaryDecoder(bytes_io)
    d = cast(dict[str, Any], reader.read(decoder))
    return Small(**d)

def ser_medium(obj: Medium) -> bytes:
    writer = avro.io.DatumWriter(MEDIUM_SCHEMA)
    bytes_io = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_io)

    d = {
        "items": [item.__dict__ for item in obj.items],
        "dict1": {str(k): v.__dict__ for k, v in obj.dict1.items()},
        "dict2": [{"key": list(k), "value": v.__dict__} for k, v in obj.dict2.items()],
        "dict3": {k: v.__dict__ for k, v in obj.dict3.items()}
    }

    writer.write(d, encoder)
    return bytes_io.getvalue()

def des_medium(data: bytes) -> Medium:
    reader = avro.io.DatumReader(MEDIUM_SCHEMA)
    bytes_io = io.BytesIO(data)
    decoder = avro.io.BinaryDecoder(bytes_io)
    d = cast(dict[str, Any], reader.read(decoder))

    items = [MediumItem(**item) for item in d['items']]
    dict1 = {int(k): Small(**v) for k, v in d['dict1'].items()}
    dict2 = {tuple(entry['key']): Small(**entry['value']) for entry in d['dict2']}
    dict3 = {k: Small(**v) for k, v in d['dict3'].items()}

    return Medium(items=items, dict1=dict1, dict2=dict2, dict3=dict3)
