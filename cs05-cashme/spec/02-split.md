There is apparently an issue on MacOS -- see the TODO comment Cargo.toml related to PyO3.

Consider a fix like:
 - instead of one library, have two
   - a pure rust library, which implements the basic structures and serializations etc,
   - and a PyO3-based library which depends on the first one and wraps the objects in a python interface,
 - the rust cli app would then _not_ depend on the second library (and not on PyO3), and thus would build fine on any platform,
 - the python code would depend on the second library, meaning we build it with maturing and rely on PyO3 (with extension-module on)

Assuming this sounds reasonable, you will thus need to split the existing library in two, and make all necessary changes to Cargo.toml, as well as to the justfile.
