Create a project skeleton for a rust library + app, and a python wheel which would pack that along with a python interface.
In particular:
 - the rust library should expose API objects, create dummy ones like `struct Request { size: u32 }; struct Response { error: Option[String], id: Option[String] }` and a `trait Message` with serialize -> [u8] method which both of the two would implement, and there would be deserialize method which returns a union over Request|Response
 - the python wheel would have methods like `new_request(size) -> Request`, `serialize_message(m) -> bytes) and `parse_message(bytes)` which just wrap the corresponding rust methods
 - the rust app for now would just be a Clap-based CLI app which can print help, and which imports the lib and eg prints a hello-world using a Response struct

Utilize just (https://github.com/casey/just) and create there recipes `test-py` (that runs ruff and pytest), `test-rs` (that runs cargo check and test), `test` (which runs both), and `build` (which builds the wheel).
The python project should be based on pyproject and uv.
The pyproject should declare that the wheel has the app as a binary that can be executed.
You could use maturin for the py-rust coop, or anything else -- as long as it works.
