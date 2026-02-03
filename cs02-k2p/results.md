```
$ ./target/release/pg_perf_tool fill table-a 50 10 10
Inserted 50 blocks in 50.433086ms
$ ./target/release/pg_perf_tool fill table-b 50 10 10
Inserted 50 blocks in 364.086502ms
$ ./target/release/pg_perf_tool fill table-c 50 10 10
Inserted 50 blocks in 4.984797891s
$ ./target/release/pg_perf_tool fill table-e 50 10 10
Inserted 50 blocks in 322.926611ms

$ ./target/release/pg_perf_tool query table-a
Query took 195.8939ms
Count: 2461
Total volume: 123104
$ ./target/release/pg_perf_tool query table-b
Query took 5.056263ms
Count: 2480
Total volume: 123301
$ ./target/release/pg_perf_tool query table-c
Query took 2.36135ms
Count: 2507
Total volume: 126535
$ ./target/release/pg_perf_tool query table-e
Query took 2.138691ms
Count: 2559
Total volume: 129175
```
