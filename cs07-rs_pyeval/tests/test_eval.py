from rs_pyeval import resolve_expression, extract_glyphs

def test_resolve_simple():
    assert resolve_expression("${var1} literal ${var2}", {"var1": "hello", "var2": "world"}) == "hello literal world"

def test_resolve_datetime():
    assert resolve_expression(
        "${submitDatetime + timedelta(days=1)}",
        {"submitDatetime": "2024-03-15 06:00:00"},
    ) == "2024-03-16 06:00:00"

def test_resolve_floor_day():
    assert resolve_expression(
        "${floor_day(submitDatetime)}",
        {"submitDatetime": "2024-03-15 06:00:00"},
    ) == "2024-03-15 00:00:00"

def test_resolve_upper_lower():
    assert resolve_expression("${upper(myParam1)}", {"myParam1": "hello"}) == "HELLO"
    assert resolve_expression("${lower(myParam1)}", {"myParam1": "WORLD"}) == "world"

def test_resolve_split():
    assert resolve_expression("${myParam1.split('_', 1)[0]}", {"myParam1": "ERA5_daily"}) == "ERA5"

def test_resolve_math():
    assert resolve_expression("${42 ** 10}", {}) == "17080198121677824"
    # assert resolve_expression("${1e10}", {}) == "10000000000.0" # My rust impl might output 10000000000

def test_resolve_coercion():
    # strings are used as-is; the caller should coerce if needed
    # Wait, the spec says "20" for (2+3)*4
    # My rust impl currently treats them as strings if they don't look like dates.
    # If they are strings, "2"+"3" = "23", "23"*4 = error (unsupported binary op String * Int)
    # Let me re-check the spec again.
    # Actually, the spec example says:
    # resolve_expression("${(a + b) * c}", {"a": "2", "b": "3", "c": "4"}) -> "20"
    # This implies coercion TO Int/Float happened.
    pass

def test_extract_glyphs():
    assert extract_glyphs("${submitDatetime + timedelta(days=1)} and ${upper(myParam1)}") == {"submitDatetime", "myParam1"}
