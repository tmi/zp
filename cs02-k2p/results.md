```
$ ./target/release/pg_perf_tool fill table_a 50 10 10
Inserted 50 blocks in 50.433086ms
$ ./target/release/pg_perf_tool fill table_b 50 10 10
Inserted 50 blocks in 364.086502ms
$ ./target/release/pg_perf_tool fill table_c 50 10 10
Inserted 50 blocks in 4.984797891s

$ ./target/release/pg_perf_tool query table_a
Query took 195.8939ms
Count: 2461
Total volume: 123104
$ ./target/release/pg_perf_tool query table_b
Query took 5.056263ms
Count: 2480
Total volume: 123301
$ ./target/release/pg_perf_tool query table_c
Query took 2.36135ms
Count: 2507
Total volume: 126535
```
