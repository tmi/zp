*Status*: Complete

# Summary

This project compared various serialization methods in Python using two dataclasses: `Small` and `Medium`.

## Results
- `orjson` and `pydantic` are very fast for serialization.
- `struct` is fast but requires manual implementation and produces fixed-length fields where possible.
- `protobuf` and `avro` offer excellent size efficiency.
- `fory` (Fury) is extremely fast for both serialization and deserialization, often outperforming others.
- `pickle` and `cloudpickle` are reliable but generally slower.

## Key Takeaways
- `orjson` is the best general-purpose JSON serializer.
- `fory` is the best for performance-critical applications.
- `protobuf` and `avro` are ideal for cross-service communication where schema is important.
