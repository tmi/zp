from ._cashme import Request, Response, new_request, serialize_message, parse_message # type: ignore

def main() -> None:
    import sys
    import subprocess
    import os
    import shutil

    # Try to find the binary named 'cashme-cli'
    bin_name = "cashme-cli"
    if sys.platform == "win32":
        bin_name += ".exe"

    # 1. Search in PATH (standard installation)
    executable_path = shutil.which(bin_name)

    # 2. Fallback: search inside the package (where we baked it)
    if not executable_path:
        pkg_dir = os.path.dirname(__file__)
        potential_pkg_bin = os.path.join(pkg_dir, bin_name)
        if os.path.exists(potential_pkg_bin):
            executable_path = potential_pkg_bin

    # 3. Fallback for development: search near the python executable (venv bin)
    if not executable_path:
        python_bin_dir = os.path.dirname(sys.executable)
        potential_bin = os.path.join(python_bin_dir, bin_name)
        if os.path.exists(potential_bin):
            executable_path = potential_bin

    # 4. Fallback for development: search in target/release or target/debug relative to the source
    if not executable_path:
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        for profile in ["release", "debug"]:
            potential_bin = os.path.join(project_root, "target", profile, bin_name)
            if os.path.exists(potential_bin):
                executable_path = potential_bin
                break

    if executable_path:
        res = subprocess.run([executable_path] + sys.argv[1:])
        sys.exit(res.returncode)
    else:
        print(f"Error: binary '{bin_name}' not found. Please ensure 'cashme' is installed correctly.", file=sys.stderr)
        sys.exit(1)

__all__ = ["Request", "Response", "new_request", "serialize_message", "parse_message", "main"]
