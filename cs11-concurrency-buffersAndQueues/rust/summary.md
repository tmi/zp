*Status*: Incubating

This subproject implements a Rust concurrency micro-benchmark evaluating message-passing performance across producer, transformer, and consumer pipelines using strategies such as lock-free slot arrays, Vyukov MPMC ring buffers, hand-rolled locking queues (`Mutex` + `Condvar`), and `crossbeam_channel` variants.
