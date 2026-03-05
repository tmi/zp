**Goal**: compare various serialization options in Python

Create a python project that defines two dataclasses:
 - `small`, containing a bunch of ints, floats, and strings,
 - `medium`, containing a list whose members are a dataclass that contains a bunch of ints, floats, strings and three dictionaries wthose values are `small` dataclass and keys are int, tuple of ints and string respectively


For each of the following, implement serialization and deserialization:
 - struct (https://docs.python.org/3/library/struct.html)
 - pickle (https://docs.python.org/3/library/pickle.html)
 - cloudpickle (https://github.com/cloudpipe/cloudpickle)
 - pydantic with mode json (https://docs.pydantic.dev/latest/concepts/serialization/)
 - orjson (https://github.com/ijl/orjson)
 - google protobuf (https://protobuf.dev/)
 - apache avro (https://avro.apache.org/docs/1.11.1/getting-started-python/)
 - apache fory (https://github.com/apache/fory)

The serialization should take a dataclass and convert it to bytes, deserialization goes back to dataclass.

Structure the code as follows:
 - schema.py -- contains the small and medium dataclasses
 - generate.py -- has methods `generate_small(n) -> list[Small]` and `generate_medium(n, k, d1, d2, d3) -> list[Medium]`. The n affects length of the outer list, the k length of inner list, and d1 d2 d3 lengths of dictionaries
 - for each of the serialization implementations, have a dedicated file `sd_<method>` with `ser_small(Small) -> bytes, ser_medium(Medium) -> bytes, des_small(bytes) -> Small, des_medium(bytes) -> Medium`
 - harness.py -- has methods
   - `unit_test(method)` which generates one sample for small and one for medium, and tests ser->des of it, comparing the result with the input for equality
   - `perf_test(method)` which generates a longer list of each and measures runtime. Don't compare for equality.

For the performance measurement:
 - generate the lists first, don't measure generation time
 - run the list through once for warmup: `for i in range(n): output_list[i] = ser(input_list[i])`.
 - then run it through five times, each time measure the total runtime using time.perf_counter_ns. Reuse the output_list between runs so that we dont pay for extra allocations
 - then do the same for deserialization
 - then repeat the whole thing but use multiprocessing.ThreadPool instead of sequential iteration

Create a file report.md with a table like this: `method | size (small-medium) | ser-mean | ser-tp-mean | des-mean | des-tp-mean`

Lastly, create a readme.md and summary.md according to AGENTS.md
