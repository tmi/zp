use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::env;
use std::hint::spin_loop;
use std::mem::MaybeUninit;
use std::process;
use std::sync::atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Instant;

use crossbeam_utils::CachePadded;

struct Xorshift64 {
    state: u64,
}

impl Xorshift64 {
    fn new(seed: u64) -> Self {
        let s = if seed == 0 { 0xda942042e4dd58b5 } else { seed };
        Self { state: s }
    }

    fn next_f32(&mut self) -> f32 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        ((x & 0x00FF_FFFF) as f32) / 16_777_216.0
    }
}

fn transform(x: &[f32], big_i: u32) -> f64 {
    let mut total = 0.0f64;
    for i in 1..=big_i {
        let mut s = 0.0f64;
        for &v in x {
            s += (v.abs() as f64).powf(i as f64);
        }
        total += s.powf(1.0 / i as f64);
    }
    total
}

#[derive(Debug, Clone)]
struct Config {
    n: usize,
    k: usize,
    i: u32,
    p: usize,
    t: usize,
    m: String,
    alloc: String,
    ring: usize,
    s: String,
    seed: u64,
}

impl Config {
    fn from_env() -> Self {
        let get_var = |name: &str| -> String {
            match env::var(name) {
                Ok(val) => val,
                Err(_) => {
                    eprintln!("Missing required environment variable: {}", name);
                    process::exit(1);
                }
            }
        };

        let n: usize = get_var("N").parse().expect("Invalid N");
        let k: usize = get_var("K").parse().expect("Invalid K");
        let i: u32 = get_var("I").parse().expect("Invalid I");
        let p: usize = get_var("P").parse().expect("Invalid P");
        let t: usize = get_var("T").parse().expect("Invalid T");
        let m = get_var("M");
        let alloc = get_var("ALLOC");
        let s = get_var("S");

        let seed: u64 = env::var("SEED")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(42);

        let ring: usize = if matches!(m.as_str(), "R" | "Qbl" | "Qbf") {
            let r: usize = get_var("RING").parse().expect("Invalid RING");
            if m == "R" && (r == 0 || (r & (r - 1)) != 0) {
                eprintln!("RING must be a power of two for method R");
                process::exit(1);
            }
            r
        } else {
            env::var("RING").ok().and_then(|v| v.parse().ok()).unwrap_or(0)
        };

        Config {
            n,
            k,
            i,
            p,
            t,
            m,
            alloc,
            ring,
            s,
            seed,
        }
    }
}

fn create_payload(k: usize, is_test: bool, rng: &mut Option<Xorshift64>, buf: &mut Vec<f32>) {
    buf.clear();
    if is_test {
        buf.resize(k, 1.0f32);
    } else {
        let rng = rng.as_mut().unwrap();
        for _ in 0..k {
            buf.push(rng.next_f32());
        }
    }
}

fn fill_payload(is_test: bool, rng: &mut Option<Xorshift64>, buf: &mut [f32]) {
    if is_test {
        buf.fill(1.0f32);
    } else {
        let rng = rng.as_mut().unwrap();
        for el in buf.iter_mut() {
            *el = rng.next_f32();
        }
    }
}

struct Pool<Tp> {
    data: Vec<UnsafeCell<Option<Tp>>>,
}
unsafe impl<Tp: Send> Sync for Pool<Tp> {}

// -----------------------------------------------------------------------------
// Method A: Lock-free slot array
// -----------------------------------------------------------------------------

struct Slots<Tp> {
    data: Vec<UnsafeCell<Option<Tp>>>,
    ready: Vec<AtomicU8>,
}

unsafe impl<Tp: Send> Sync for Slots<Tp> {}

fn run_slot_array(config: &Config, is_test: bool) {
    let mut pslots_data = Vec::with_capacity(config.n);
    let mut ready_vec = Vec::with_capacity(config.n);
    let mut cval_vec = Vec::with_capacity(config.n);
    let mut cready_vec = Vec::with_capacity(config.n);

    for _ in 0..config.n {
        if config.alloc == "pool" {
            pslots_data.push(UnsafeCell::new(Some(vec![0.0f32; config.k])));
        } else {
            pslots_data.push(UnsafeCell::new(None));
        }
        ready_vec.push(AtomicU8::new(0));
        cval_vec.push(AtomicU64::new(0));
        cready_vec.push(AtomicU8::new(0));
    }

    let pslots = Arc::new(Slots {
        data: pslots_data,
        ready: ready_vec,
    });
    let cval = Arc::new(cval_vec);
    let cready = Arc::new(cready_vec);
    let in_claim = Arc::new(CachePadded::new(AtomicUsize::new(0)));

    let n = config.n;
    let k = config.k;
    let big_i = config.i;
    let p_cnt = config.p;
    let t_cnt = config.t;
    let alloc_mode = config.alloc.clone();
    let seed = config.seed;

    // Consumer thread
    let cval_cons = Arc::clone(&cval);
    let cready_cons = Arc::clone(&cready);
    let consumer_handle = thread::spawn(move || {
        let mut sum = 0.0f64;
        for idx in 0..n {
            while cready_cons[idx].load(Ordering::Acquire) == 0 {
                spin_loop();
            }
            sum += f64::from_bits(cval_cons[idx].load(Ordering::Relaxed));
        }
        let end = Instant::now();
        (end, sum)
    });

    // Transformer threads
    let mut transformer_handles = Vec::with_capacity(t_cnt);
    for _ in 0..t_cnt {
        let in_claim = Arc::clone(&in_claim);
        let pslots = Arc::clone(&pslots);
        let cval = Arc::clone(&cval);
        let cready = Arc::clone(&cready);
        transformer_handles.push(thread::spawn(move || loop {
            let idx = in_claim.fetch_add(1, Ordering::Relaxed);
            if idx >= n {
                break;
            }
            while pslots.ready[idx].load(Ordering::Acquire) == 0 {
                spin_loop();
            }
            let buf = unsafe { (*pslots.data[idx].get()).take().unwrap() };
            let out = transform(&buf, big_i);
            cval[idx].store(out.to_bits(), Ordering::Relaxed);
            cready[idx].store(1, Ordering::Release);
        }));
    }

    let start = Instant::now();

    // Producer threads
    let mut producer_handles = Vec::with_capacity(p_cnt);
    for p in 0..p_cnt {
        let pslots = Arc::clone(&pslots);
        let alloc_mode = alloc_mode.clone();
        producer_handles.push(thread::spawn(move || {
            let p_start = p * n / p_cnt;
            let p_end = (p + 1) * n / p_cnt;
            let mut rng = if is_test {
                None
            } else {
                Some(Xorshift64::new(seed + p as u64))
            };

            for id in p_start..p_end {
                if alloc_mode == "pool" {
                    let buf_ptr = unsafe { (*pslots.data[id].get()).as_mut().unwrap() };
                    fill_payload(is_test, &mut rng, buf_ptr);
                } else {
                    let mut buf = Vec::with_capacity(k);
                    create_payload(k, is_test, &mut rng, &mut buf);
                    unsafe {
                        *pslots.data[id].get() = Some(buf);
                    }
                }
                pslots.ready[id].store(1, Ordering::Release);
            }
        }));
    }

    for h in producer_handles {
        h.join().unwrap();
    }
    for h in transformer_handles {
        h.join().unwrap();
    }

    let (end, sum) = consumer_handle.join().unwrap();
    let ns = (end - start).as_nanos();
    println!("RESULT {} {:.6}", ns, sum);
}

// -----------------------------------------------------------------------------
// Method R: Vyukov bounded MPMC ring buffer
// -----------------------------------------------------------------------------

struct Cell<Tp> {
    seq: AtomicUsize,
    data: UnsafeCell<MaybeUninit<Tp>>,
}

struct Ring<Tp> {
    buf: Box<[Cell<Tp>]>,
    mask: usize,
    enq: CachePadded<AtomicUsize>,
    deq: CachePadded<AtomicUsize>,
}

unsafe impl<Tp: Send> Sync for Ring<Tp> {}

impl<Tp> Ring<Tp> {
    fn new(cap: usize) -> Self {
        assert!(cap > 0 && (cap & (cap - 1)) == 0, "Capacity must be power of two");
        let mut buf_vec = Vec::with_capacity(cap);
        for i in 0..cap {
            buf_vec.push(Cell {
                seq: AtomicUsize::new(i),
                data: UnsafeCell::new(MaybeUninit::uninit()),
            });
        }
        Ring {
            buf: buf_vec.into_boxed_slice(),
            mask: cap - 1,
            enq: CachePadded::new(AtomicUsize::new(0)),
            deq: CachePadded::new(AtomicUsize::new(0)),
        }
    }

    fn enqueue(&self, item: Tp) -> Result<(), Tp> {
        let mut pos = self.enq.load(Ordering::Relaxed);
        loop {
            let cell = &self.buf[pos & self.mask];
            let seq = cell.seq.load(Ordering::Acquire);
            let dif = seq as isize - pos as isize;
            if dif == 0 {
                if self
                    .enq
                    .compare_exchange_weak(pos, pos + 1, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
                {
                    unsafe {
                        (*cell.data.get()).write(item);
                    }
                    cell.seq.store(pos + 1, Ordering::Release);
                    return Ok(());
                }
            } else if dif < 0 {
                return Err(item);
            } else {
                pos = self.enq.load(Ordering::Relaxed);
            }
        }
    }

    fn dequeue(&self) -> Option<Tp> {
        let mut pos = self.deq.load(Ordering::Relaxed);
        loop {
            let cell = &self.buf[pos & self.mask];
            let seq = cell.seq.load(Ordering::Acquire);
            let dif = seq as isize - (pos + 1) as isize;
            if dif == 0 {
                if self
                    .deq
                    .compare_exchange_weak(pos, pos + 1, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
                {
                    let item = unsafe { (*cell.data.get()).assume_init_read() };
                    cell.seq.store(pos + self.mask + 1, Ordering::Release);
                    return Some(item);
                }
            } else if dif < 0 {
                return None;
            } else {
                pos = self.deq.load(Ordering::Relaxed);
            }
        }
    }
}

fn run_ring(config: &Config, is_test: bool) {
    let in_ring: Arc<Ring<Vec<f32>>> = Arc::new(Ring::new(config.ring));
    let out_ring: Arc<Ring<f64>> = Arc::new(Ring::new(config.ring));
    let in_claim = Arc::new(CachePadded::new(AtomicUsize::new(0)));

    let n = config.n;
    let k = config.k;
    let big_i = config.i;
    let p_cnt = config.p;
    let t_cnt = config.t;
    let alloc_mode = config.alloc.clone();
    let seed = config.seed;

    let mut pool = Arc::new(Pool { data: Vec::new() });
    if alloc_mode == "pool" {
        let mut p_vec = Vec::with_capacity(n);
        for _ in 0..n {
            p_vec.push(UnsafeCell::new(Some(vec![0.0f32; k])));
        }
        pool = Arc::new(Pool { data: p_vec });
    }

    // Consumer thread
    let out_ring_cons = Arc::clone(&out_ring);
    let consumer_handle = thread::spawn(move || {
        let mut sum = 0.0f64;
        for _ in 0..n {
            let out = loop {
                if let Some(val) = out_ring_cons.dequeue() {
                    break val;
                }
                spin_loop();
            };
            sum += out;
        }
        let end = Instant::now();
        (end, sum)
    });

    // Transformer threads
    let mut transformer_handles = Vec::with_capacity(t_cnt);
    for _ in 0..t_cnt {
        let in_claim = Arc::clone(&in_claim);
        let in_ring = Arc::clone(&in_ring);
        let out_ring = Arc::clone(&out_ring);
        transformer_handles.push(thread::spawn(move || loop {
            let idx = in_claim.fetch_add(1, Ordering::Relaxed);
            if idx >= n {
                break;
            }
            let buf = loop {
                if let Some(b) = in_ring.dequeue() {
                    break b;
                }
                spin_loop();
            };
            let out = transform(&buf, big_i);
            let mut to_send = out;
            loop {
                match out_ring.enqueue(to_send) {
                    Ok(()) => break,
                    Err(returned) => {
                        to_send = returned;
                        spin_loop();
                    }
                }
            }
        }));
    }

    let start = Instant::now();

    // Producer threads
    let mut producer_handles = Vec::with_capacity(p_cnt);
    for p in 0..p_cnt {
        let in_ring = Arc::clone(&in_ring);
        let pool = Arc::clone(&pool);
        let alloc_mode = alloc_mode.clone();
        producer_handles.push(thread::spawn(move || {
            let p_start = p * n / p_cnt;
            let p_end = (p + 1) * n / p_cnt;
            let mut rng = if is_test {
                None
            } else {
                Some(Xorshift64::new(seed + p as u64))
            };

            for id in p_start..p_end {
                let mut buf = if alloc_mode == "pool" {
                    let mut b = unsafe { (*pool.data[id].get()).take().unwrap() };
                    fill_payload(is_test, &mut rng, &mut b);
                    b
                } else {
                    let mut b = Vec::with_capacity(k);
                    create_payload(k, is_test, &mut rng, &mut b);
                    b
                };

                loop {
                    match in_ring.enqueue(buf) {
                        Ok(()) => break,
                        Err(returned) => {
                            buf = returned;
                            spin_loop();
                        }
                    }
                }
            }
        }));
    }

    for h in producer_handles {
        h.join().unwrap();
    }
    for h in transformer_handles {
        h.join().unwrap();
    }

    let (end, sum) = consumer_handle.join().unwrap();
    let ns = (end - start).as_nanos();
    println!("RESULT {} {:.6}", ns, sum);
}

// -----------------------------------------------------------------------------
// Method Qbl & Qul: Locking queues (Mutex + Condvar)
// -----------------------------------------------------------------------------

struct BoundedLockingQueue<Tp> {
    queue: Mutex<VecDeque<Tp>>,
    cap: usize,
    not_full: Condvar,
    not_empty: Condvar,
}

impl<Tp> BoundedLockingQueue<Tp> {
    fn new(cap: usize) -> Self {
        Self {
            queue: Mutex::new(VecDeque::with_capacity(cap)),
            cap,
            not_full: Condvar::new(),
            not_empty: Condvar::new(),
        }
    }

    fn push(&self, item: Tp) {
        let mut q = self.queue.lock().unwrap();
        q = self.not_full.wait_while(q, |q| q.len() >= self.cap).unwrap();
        q.push_back(item);
        drop(q);
        self.not_empty.notify_one();
    }

    fn pop(&self) -> Tp {
        let mut q = self.queue.lock().unwrap();
        q = self.not_empty.wait_while(q, |q| q.is_empty()).unwrap();
        let item = q.pop_front().unwrap();
        drop(q);
        self.not_full.notify_one();
        item
    }
}

struct UnboundedLockingQueue<Tp> {
    queue: Mutex<VecDeque<Tp>>,
    not_empty: Condvar,
}

impl<Tp> UnboundedLockingQueue<Tp> {
    fn new() -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            not_empty: Condvar::new(),
        }
    }

    fn push(&self, item: Tp) {
        let mut q = self.queue.lock().unwrap();
        q.push_back(item);
        drop(q);
        self.not_empty.notify_one();
    }

    fn pop(&self) -> Tp {
        let mut q = self.queue.lock().unwrap();
        q = self.not_empty.wait_while(q, |q| q.is_empty()).unwrap();
        let item = q.pop_front().unwrap();
        drop(q);
        item
    }
}

enum LockingQueue<Tp> {
    Bounded(BoundedLockingQueue<Tp>),
    Unbounded(UnboundedLockingQueue<Tp>),
}

impl<Tp> LockingQueue<Tp> {
    fn push(&self, item: Tp) {
        match self {
            LockingQueue::Bounded(q) => q.push(item),
            LockingQueue::Unbounded(q) => q.push(item),
        }
    }

    fn pop(&self) -> Tp {
        match self {
            LockingQueue::Bounded(q) => q.pop(),
            LockingQueue::Unbounded(q) => q.pop(),
        }
    }
}

fn run_locking_queue(config: &Config, is_test: bool, bounded: bool) {
    let in_q: Arc<LockingQueue<Vec<f32>>> = if bounded {
        Arc::new(LockingQueue::Bounded(BoundedLockingQueue::new(config.ring)))
    } else {
        Arc::new(LockingQueue::Unbounded(UnboundedLockingQueue::new()))
    };

    let out_q: Arc<LockingQueue<f64>> = if bounded {
        Arc::new(LockingQueue::Bounded(BoundedLockingQueue::new(config.ring)))
    } else {
        Arc::new(LockingQueue::Unbounded(UnboundedLockingQueue::new()))
    };

    let in_claim = Arc::new(CachePadded::new(AtomicUsize::new(0)));

    let n = config.n;
    let k = config.k;
    let big_i = config.i;
    let p_cnt = config.p;
    let t_cnt = config.t;
    let alloc_mode = config.alloc.clone();
    let seed = config.seed;

    let mut pool = Arc::new(Pool { data: Vec::new() });
    if alloc_mode == "pool" {
        let mut p_vec = Vec::with_capacity(n);
        for _ in 0..n {
            p_vec.push(UnsafeCell::new(Some(vec![0.0f32; k])));
        }
        pool = Arc::new(Pool { data: p_vec });
    }

    // Consumer thread
    let out_q_cons = Arc::clone(&out_q);
    let consumer_handle = thread::spawn(move || {
        let mut sum = 0.0f64;
        for _ in 0..n {
            sum += out_q_cons.pop();
        }
        let end = Instant::now();
        (end, sum)
    });

    // Transformer threads
    let mut transformer_handles = Vec::with_capacity(t_cnt);
    for _ in 0..t_cnt {
        let in_claim = Arc::clone(&in_claim);
        let in_q = Arc::clone(&in_q);
        let out_q = Arc::clone(&out_q);
        transformer_handles.push(thread::spawn(move || loop {
            let idx = in_claim.fetch_add(1, Ordering::Relaxed);
            if idx >= n {
                break;
            }
            let buf = in_q.pop();
            let out = transform(&buf, big_i);
            out_q.push(out);
        }));
    }

    let start = Instant::now();

    // Producer threads
    let mut producer_handles = Vec::with_capacity(p_cnt);
    for p in 0..p_cnt {
        let in_q = Arc::clone(&in_q);
        let pool = Arc::clone(&pool);
        let alloc_mode = alloc_mode.clone();
        producer_handles.push(thread::spawn(move || {
            let p_start = p * n / p_cnt;
            let p_end = (p + 1) * n / p_cnt;
            let mut rng = if is_test {
                None
            } else {
                Some(Xorshift64::new(seed + p as u64))
            };

            for id in p_start..p_end {
                let buf = if alloc_mode == "pool" {
                    let mut b = unsafe { (*pool.data[id].get()).take().unwrap() };
                    fill_payload(is_test, &mut rng, &mut b);
                    b
                } else {
                    let mut b = Vec::with_capacity(k);
                    create_payload(k, is_test, &mut rng, &mut b);
                    b
                };
                in_q.push(buf);
            }
        }));
    }

    for h in producer_handles {
        h.join().unwrap();
    }
    for h in transformer_handles {
        h.join().unwrap();
    }

    let (end, sum) = consumer_handle.join().unwrap();
    let ns = (end - start).as_nanos();
    println!("RESULT {} {:.6}", ns, sum);
}

// -----------------------------------------------------------------------------
// Method Qbf & Quf: Lock-free queues (crossbeam-channel)
// -----------------------------------------------------------------------------

fn run_crossbeam_channel(config: &Config, is_test: bool, bounded: bool) {
    let (in_tx, in_rx) = if bounded {
        crossbeam_channel::bounded::<Vec<f32>>(config.ring)
    } else {
        crossbeam_channel::unbounded::<Vec<f32>>()
    };

    let (out_tx, out_rx) = if bounded {
        crossbeam_channel::bounded::<f64>(config.ring)
    } else {
        crossbeam_channel::unbounded::<f64>()
    };

    let in_claim = Arc::new(CachePadded::new(AtomicUsize::new(0)));

    let n = config.n;
    let k = config.k;
    let big_i = config.i;
    let p_cnt = config.p;
    let t_cnt = config.t;
    let alloc_mode = config.alloc.clone();
    let seed = config.seed;

    let mut pool = Arc::new(Pool { data: Vec::new() });
    if alloc_mode == "pool" {
        let mut p_vec = Vec::with_capacity(n);
        for _ in 0..n {
            p_vec.push(UnsafeCell::new(Some(vec![0.0f32; k])));
        }
        pool = Arc::new(Pool { data: p_vec });
    }

    // Consumer thread
    let consumer_handle = thread::spawn(move || {
        let mut sum = 0.0f64;
        for _ in 0..n {
            let out = out_rx.recv().unwrap();
            sum += out;
        }
        let end = Instant::now();
        (end, sum)
    });

    // Transformer threads
    let mut transformer_handles = Vec::with_capacity(t_cnt);
    for _ in 0..t_cnt {
        let in_claim = Arc::clone(&in_claim);
        let in_rx = in_rx.clone();
        let out_tx = out_tx.clone();
        transformer_handles.push(thread::spawn(move || loop {
            let idx = in_claim.fetch_add(1, Ordering::Relaxed);
            if idx >= n {
                break;
            }
            let buf = in_rx.recv().unwrap();
            let out = transform(&buf, big_i);
            out_tx.send(out).unwrap();
        }));
    }

    let start = Instant::now();

    // Producer threads
    let mut producer_handles = Vec::with_capacity(p_cnt);
    for p in 0..p_cnt {
        let in_tx = in_tx.clone();
        let pool = Arc::clone(&pool);
        let alloc_mode = alloc_mode.clone();
        producer_handles.push(thread::spawn(move || {
            let p_start = p * n / p_cnt;
            let p_end = (p + 1) * n / p_cnt;
            let mut rng = if is_test {
                None
            } else {
                Some(Xorshift64::new(seed + p as u64))
            };

            for id in p_start..p_end {
                let buf = if alloc_mode == "pool" {
                    let mut b = unsafe { (*pool.data[id].get()).take().unwrap() };
                    fill_payload(is_test, &mut rng, &mut b);
                    b
                } else {
                    let mut b = Vec::with_capacity(k);
                    create_payload(k, is_test, &mut rng, &mut b);
                    b
                };
                in_tx.send(buf).unwrap();
            }
        }));
    }

    drop(in_tx);
    drop(out_tx);

    for h in producer_handles {
        h.join().unwrap();
    }
    for h in transformer_handles {
        h.join().unwrap();
    }

    let (end, sum) = consumer_handle.join().unwrap();
    let ns = (end - start).as_nanos();
    println!("RESULT {} {:.6}", ns, sum);
}

// -----------------------------------------------------------------------------
// Method B: Baseline (Serial execution)
// -----------------------------------------------------------------------------

fn run_baseline(config: &Config, is_test: bool) {
    let mut pool: Vec<Vec<f32>> = Vec::new();
    if config.alloc == "pool" {
        pool.reserve(config.n);
        for _ in 0..config.n {
            pool.push(vec![0.0f32; config.k]);
        }
    }

    let start = Instant::now();
    let mut sum = 0.0f64;
    let mut rng = if is_test {
        None
    } else {
        Some(Xorshift64::new(config.seed))
    };

    if config.alloc == "pool" {
        for idx in 0..config.n {
            fill_payload(is_test, &mut rng, &mut pool[idx]);
            sum += transform(&pool[idx], config.i);
        }
    } else {
        let mut buf = Vec::with_capacity(config.k);
        for _ in 0..config.n {
            create_payload(config.k, is_test, &mut rng, &mut buf);
            sum += transform(&buf, config.i);
        }
    }

    let end = Instant::now();
    let ns = (end - start).as_nanos();
    println!("RESULT {} {:.6}", ns, sum);
}

fn run_single(config: &Config, is_test: bool) {
    match config.m.as_str() {
        "B" => run_baseline(config, is_test),
        "A" => run_slot_array(config, is_test),
        "R" => run_ring(config, is_test),
        "Qbl" => run_locking_queue(config, is_test, true),
        "Qul" => run_locking_queue(config, is_test, false),
        "Qbf" => run_crossbeam_channel(config, is_test, true),
        "Quf" => run_crossbeam_channel(config, is_test, false),
        _ => {
            eprintln!("Unknown method: {}", config.m);
            process::exit(1);
        }
    }
}

fn main() {
    let config = Config::from_env();

    if config.s == "test" {
        run_single(&config, true);
    } else {
        let parts: Vec<&str> = config.s.split('/').collect();
        if parts.len() != 2 {
            eprintln!("Invalid S format. Expected 'test' or '<warmup>/<real>'");
            process::exit(1);
        }
        let warmup: usize = parts[0].parse().expect("Invalid warmup count");
        let real: usize = parts[1].parse().expect("Invalid real count");

        for _ in 0..warmup {
            run_single(&config, false);
        }
        println!("---");
        for _ in 0..real {
            run_single(&config, false);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transform_oracle() {
        let k = 64;
        let big_i = 4;
        let input = vec![1.0f32; k];
        let val = transform(&input, big_i);

        let mut expected = 0.0f64;
        for i in 1..=big_i {
            let mut s = 0.0f64;
            for &v in &input {
                s += (v.abs() as f64).powf(i as f64);
            }
            expected += s.powf(1.0 / i as f64);
        }

        assert!((val - expected).abs() < 1e-9);
    }

    #[test]
    fn test_xorshift64() {
        let mut rng1 = Xorshift64::new(42);
        let mut rng2 = Xorshift64::new(42);
        for _ in 0..100 {
            let v1 = rng1.next_f32();
            let v2 = rng2.next_f32();
            assert_eq!(v1, v2);
            assert!((0.0..1.0).contains(&v1));
        }
    }

    #[test]
    fn test_ring_enqueue_dequeue() {
        let ring = Ring::<i32>::new(4);
        assert!(ring.dequeue().is_none());
        assert!(ring.enqueue(10).is_ok());
        assert!(ring.enqueue(20).is_ok());
        assert!(ring.enqueue(30).is_ok());
        assert!(ring.enqueue(40).is_ok());
        assert_eq!(ring.enqueue(50), Err(50)); // full

        assert_eq!(ring.dequeue(), Some(10));
        assert_eq!(ring.dequeue(), Some(20));
        assert!(ring.enqueue(50).is_ok());
        assert_eq!(ring.dequeue(), Some(30));
        assert_eq!(ring.dequeue(), Some(40));
        assert_eq!(ring.dequeue(), Some(50));
        assert!(ring.dequeue().is_none());
    }

    #[test]
    fn test_locking_queues() {
        let bq = BoundedLockingQueue::new(2);
        bq.push(1);
        bq.push(2);
        assert_eq!(bq.pop(), 1);
        bq.push(3);
        assert_eq!(bq.pop(), 2);
        assert_eq!(bq.pop(), 3);

        let uq = UnboundedLockingQueue::new();
        uq.push(10);
        uq.push(20);
        assert_eq!(uq.pop(), 10);
        assert_eq!(uq.pop(), 20);
    }
}
