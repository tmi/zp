Read the README.md to understand the project.

You will be adding a new table schemata, like table_b having one row per OuterStruct, but not using jsonb for array so that we dont need to parse the jsonb at query time:
 - table_d: utilize ability of postgres to have arrays of complex types to represent inner struct
 - table_e: break down inner struct into individual primitive columns and have each as its own array -- meaning the table would have real[] threshold, int[] value, string[] innerId.

Implement create, clean, fill and query for both new tables.

Verify first with cargo check, and then run the tool (the docker image is running already).

At the end, commit but dont push.
