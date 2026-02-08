import pytest
from harness import unit_test

@pytest.mark.parametrize("method", ["struct", "pickle", "cloudpickle", "pydantic", "orjson", "protobuf", "avro", "fory"])
def test_method(method):
    unit_test(method)
