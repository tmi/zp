Implement a rust library with python bindings using maturin, for evaluating expressions.

The library will expose two functions:
 - `resolve_expression(raw: str, variables: dict[str, str]) -> str`
 - `extract_glyphs(raw: str) -> set[str]`

## Reference Implementation
Resides in the file 01-reference.py in this folder.

Write unit tests using the examples below.
Make sure tests exist both as rust and python implementations.
The `just` recipe for validation should invoke both of these in sequence.

## Examples

```python
resolve_expression("${var1} literal ${var2}", {"var1": "hello", "var2": "world"})
# → "hello literal world"

resolve_expression(
    "${submitDatetime + timedelta(days=1)}",
    {"submitDatetime": "2024-03-15 06:00:00"},
)
# → "2024-03-16 06:00:00"

resolve_expression(
    "${floor_day(submitDatetime)}",
    {"submitDatetime": "2024-03-15 06:00:00"},
)
# → "2024-03-15 00:00:00"

resolve_expression("${upper(myParam1)}", {"myParam1": "hello"})
# → "HELLO"

resolve_expression("${lower(myParam1)}", {"myParam1": "WORLD"})
# → "world"

resolve_expression("${myParam1.split('_', 1)[0]}", {"myParam1": "ERA5_daily"})
# → "ERA5"

resolve_expression("${42 ** 10}", {})
# → "17080198121677824"

resolve_expression("${1e10}", {})
# → "10000000000.0"

resolve_expression("${(a + b) * c}", {"a": "2", "b": "3", "c": "4"})
# → "20"  (strings are used as-is; the caller should coerce if needed)

extract_glyphs("${submitDatetime + timedelta(days=1)} and ${upper(myParam1)}")
# → {"submitDatetime", "myParam1"}
# `timedelta` and `upper` are built-in names and are excluded from the result.
```
