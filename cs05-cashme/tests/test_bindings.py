import subprocess
import cashme

def test_request():
    req = cashme.new_request(10)
    assert req.size == 10
    assert isinstance(req, cashme.Request)

def test_serialization():
    req = cashme.new_request(42)
    bytes_data = cashme.serialize_message(req)
    parsed = cashme.parse_message(bytes_data)
    assert isinstance(parsed, cashme.Request)
    assert parsed.size == 42

def test_response_serialization():
    res = cashme.Response(error="oops", id="777")
    bytes_data = cashme.serialize_message(res)
    parsed = cashme.parse_message(bytes_data)
    assert isinstance(parsed, cashme.Response)
    assert parsed.error == "oops"
    assert parsed.id == "777"

def test_cli():
    # Maturin installs the binary in the .venv/bin folder.
    # When running with 'uv run', it should be in the path.
    # Actually, we can call 'cashme' as defined in project.scripts, or the rust binary directly.
    # But let's check if 'cashme' command works.

    # Try just 'cashme'
    result = subprocess.run(["cashme"], capture_output=True, text=True)
    if result.returncode != 0:
        # Try 'uv run cashme'
        result = subprocess.run(["uv", "run", "cashme"], capture_output=True, text=True)

    assert result.returncode == 0
    assert "Hello World:" in result.stdout
    assert "Response" in result.stdout
    assert "hello-world" in result.stdout
