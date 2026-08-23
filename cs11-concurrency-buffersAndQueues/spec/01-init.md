I want to study and compare two particular concurrency approaches against a baseline, and across languages.

We have three components:
1. Producer, emitting middle-sized messages at a regular pace,
2. Transformer, who is doing CPU intensive work,
3. Consumer, which is basically just checking the output.

In a real scenario, Producer be partially I/O bound (plus CPU-bound serde), but for our case we stick to them having no I/O, to reduce measurement variance.
It should be that Consumer is fastest, Producer (inevitably) a bit slower, and Transformer slow.

For example, the message schema would be an array of float32 of length K, and then:
1. Producer would only construct such random array and pass it on,
2. Transformer would do some I arithmetic operations -- like calculating Li norm for i in (1..I) of the array and sum all the norms,
3. Consumer would keep track of the total sum of all messages.

We are interested in three concurrency implementations as well as baseline:
1. Baseline -- no concurrency at all. Basically
```
for i in 0..N:
    in = producer(i)
    out = transformer(in)
    consumer(out)
```
2. Queue -- using ideally the language stdlib's Queue, or a canonical implementation thereof:
```
# init
atomic_total = N
# producers (configurable amount, P of them, no need to coordinate mutually, each produces a static segment of 0..N)
for i in seg_start..seg_end:
    producer_queue.push(producer(i))
# transformers (configurable amount, T of them, they coordinate via the queue + atomic_total variable)
while atomic_total > 0:
    in = producer_queue.pop()
    out = transformer(in)
    consumer_queue.push(out)
    atomic_total.decrement()
# consumers (just a single thread)
for i in 0..N:
    consumer(consumer_queue.pop())
```
3. Volatile Array + Atomic Variables for coordination + Sleeps instead of Polling
```
producer_array = new volatile array [N], initialize to nulls
consumer_array = new volatile array [N], initialize to nulls
producer_w_idx = 0 # atomic variable
producer_r_idx = 0 # atomic variable
consumer_w_idx = 0 # atomic variable
consumer_r_idx = 0 # atomic variable (in this actually doest need to be but lets be consistent)
# producer (P of them)
for i in seg_start..seg_end:
    in = producer(i)
    producer_array[producer_w_idx++] = in 
# transformer (T of them)
loop:
    idx = producer_r_idx++
    if idx >= N: break
    while producer_array[idx] is null:
        tiny_sleep
    out = transform(producer_array[idx])
    idx = consumer_w_idx++
    consumer_array[idx] = out
# consumer (1)
for consumer_r_idx in 0..N:
    while consumer_array[consumer_r_idx] is null:
        tiny_sleep
    consumer(consumer_array[consumer_r_idx])
```
4. Volatile Ring Buffer -- like previous, but instead of size N, the arrays would be of size M << N, and the read/write logic needs to be more complicated:
```
# read
idx = (_r_idx++) % M;
while _array[idx] is null:
    tiny_sleep
value = _array[idx]
_array[idx] = null # we null the input to signify it has been processed!
# write
idx = _w_idx++;
while _r_idx <= idx: # we need to wait until the readers have progressed beyond our write point
    tiny_sleep
while _array[idx] is not null: # the reader's index may have progressed, but only the value being null signifies the value has been retrieved actually
    tiny_sleep
_array[idx] = value
```

We do *not* care about being nice with CPU utilization -- busy waits are ok, even preferred, over any sort of polling, sleeping, messaging.
It is assumed that in the real case, the system will have enough cores to run P+T+1 threads pinned.
Use manual thread management, not pools.

We care for total runtime, measured as follows:
1. Use system precise monotic time (eg python's perf_counter_ns, Java's System.nanoTime, rust's std::time::Instant::now)
2. Mark start in the main thread right before you spawn the first producer thread (or hand over in the baseline case)
3. Mark end in the (single) consumer after the last message is processed, output the difference

We do not care about ordering of messages at the output, as consumer only calculates sum (and say we don't care about precision high enough for associativity to matter).
But we care about not losing any message -- you need to allow for producers to disable randomness and produce messages of value uniformly 1.
Then we can compare that all implementations give the same result in consumer -- the consumer should output the value after it outputs the time.

You should produce a binary which is completely driven by envvars -- N (message count), K (message length), I (transformer complexity), P (producer count), T (transformer count), M (method -- say value "B" stands for baseline, "Q" for queue, "A" for array, "R" for ring buffer), S (for suite -- lets say value "test" would do one run with the all-1 inputs, and otherwise it would be a string of "<warmupRuns>/<realRuns>", executed in a sequence, with a `---` marker after the warmup runs).
