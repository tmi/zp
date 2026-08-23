import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

public class Bench {

    // Cache-line padded AtomicLong to avoid false sharing
    static final class PaddedAtomicLong extends AtomicLong {
        @SuppressWarnings("unused")
        volatile long p1, p2, p3, p4, p5, p6, p7;

        public PaddedAtomicLong() {
            super();
        }

        public PaddedAtomicLong(long initialValue) {
            super(initialValue);
        }
    }

    // Vyukov MPMC ring buffer for float[] payload
    static final class VyukovRingFloat {
        final int cap;
        final int mask;
        final AtomicLongArray seq;
        final Object[] data;
        final PaddedAtomicLong enqPos = new PaddedAtomicLong(0);
        final PaddedAtomicLong deqPos = new PaddedAtomicLong(0);

        VyukovRingFloat(int cap) {
            this.cap = cap;
            this.mask = cap - 1;
            this.seq = new AtomicLongArray(cap);
            for (int i = 0; i < cap; i++) {
                seq.set(i, i);
            }
            this.data = new Object[cap];
        }

        boolean enqueue(float[] item) {
            long pos = enqPos.get();
            for (;;) {
                int j = (int) (pos & mask);
                long dif = seq.get(j) - pos;
                if (dif == 0) {
                    if (enqPos.compareAndSet(pos, pos + 1)) break;
                } else if (dif < 0) {
                    return false;
                } else {
                    pos = enqPos.get();
                }
            }
            data[(int) (pos & mask)] = item;
            seq.set((int) (pos & mask), pos + 1);
            return true;
        }

        float[] dequeue() {
            long pos = deqPos.get();
            for (;;) {
                int j = (int) (pos & mask);
                long dif = seq.get(j) - (pos + 1);
                if (dif == 0) {
                    if (deqPos.compareAndSet(pos, pos + 1)) break;
                } else if (dif < 0) {
                    return null;
                } else {
                    pos = deqPos.get();
                }
            }
            int idx = (int) (pos & mask);
            float[] res = (float[]) data[idx];
            data[idx] = null;
            seq.set(idx, pos + cap);
            return res;
        }
    }

    // Vyukov MPMC ring buffer for double payload
    static final class VyukovRingDouble {
        final int cap;
        final int mask;
        final AtomicLongArray seq;
        final double[] data;
        final PaddedAtomicLong enqPos = new PaddedAtomicLong(0);
        final PaddedAtomicLong deqPos = new PaddedAtomicLong(0);

        VyukovRingDouble(int cap) {
            this.cap = cap;
            this.mask = cap - 1;
            this.seq = new AtomicLongArray(cap);
            for (int i = 0; i < cap; i++) {
                seq.set(i, i);
            }
            this.data = new double[cap];
        }

        boolean enqueue(double item) {
            long pos = enqPos.get();
            for (;;) {
                int j = (int) (pos & mask);
                long dif = seq.get(j) - pos;
                if (dif == 0) {
                    if (enqPos.compareAndSet(pos, pos + 1)) break;
                } else if (dif < 0) {
                    return false;
                } else {
                    pos = enqPos.get();
                }
            }
            data[(int) (pos & mask)] = item;
            seq.set((int) (pos & mask), pos + 1);
            return true;
        }

        Double dequeue() {
            long pos = deqPos.get();
            for (;;) {
                int j = (int) (pos & mask);
                long dif = seq.get(j) - (pos + 1);
                if (dif == 0) {
                    if (deqPos.compareAndSet(pos, pos + 1)) break;
                } else if (dif < 0) {
                    return null;
                } else {
                    pos = deqPos.get();
                }
            }
            int idx = (int) (pos & mask);
            double res = data[idx];
            seq.set(idx, pos + cap);
            return res;
        }
    }

    // Xorshift PRNG (64-bit)
    static long xorshift64(long state) {
        state ^= (state << 13);
        state ^= (state >>> 7);
        state ^= (state << 17);
        return state;
    }

    static void fillMessage(float[] buf, int id, boolean isTest, long seed, int pIndex) {
        if (isTest) {
            for (int i = 0; i < buf.length; i++) {
                buf[i] = 1.0f;
            }
        } else {
            // Cheap PRNG seeded from SEED + producerIndex
            // Use state per message based on id and producer seed
            long state = seed + pIndex + id * 31L;
            if (state == 0) state = 0xDEADBEEFL;
            for (int i = 0; i < buf.length; i++) {
                state = xorshift64(state);
                // Map positive long to [0, 1) float
                float val = (float) ((state >>> 11) & 0x1FFFFFFFFFFFFFL) / (float) (1L << 53);
                buf[i] = Math.abs(val % 1.0f);
            }
        }
    }

    // Transformer calculation
    static double transform(float[] x, int I) {
        double total = 0.0;
        for (int i = 1; i <= I; i++) {
            double s = 0.0;
            for (int k = 0; k < x.length; k++) {
                s += Math.pow(Math.abs((double) x[k]), (double) i);
            }
            total += Math.pow(s, 1.0 / i);
        }
        return total;
    }

    public static void main(String[] args) {
        // Parse environment variables
        String envN = System.getenv("N");
        String envK = System.getenv("K");
        String envI = System.getenv("I");
        String envP = System.getenv("P");
        String envT = System.getenv("T");
        String envM = System.getenv("M");
        String envALLOC = System.getenv("ALLOC");
        String envRING = System.getenv("RING");
        String envS = System.getenv("S");
        String envSEED = System.getenv("SEED");

        if (envN == null || envK == null || envI == null || envP == null || envT == null ||
            envM == null || envALLOC == null || envS == null) {
            System.err.println("Error: Missing required environment variables.");
            System.exit(1);
        }

        if (envM.equals("Qbf")) {
            System.err.println("Error: Qbf method is explicitly unsupported.");
            System.exit(1);
        }

        long N = Long.parseLong(envN);
        int K = Integer.parseInt(envK);
        int I = Integer.parseInt(envI);
        int P = Integer.parseInt(envP);
        int T = Integer.parseInt(envT);
        String M = envM;
        String ALLOC = envALLOC;
        int RING = envRING != null ? Integer.parseInt(envRING) : 1024;
        long SEED = envSEED != null ? Long.parseLong(envSEED) : 42L;
        String S = envS;

        if (M.equals("R") || M.equals("Qbl")) {
            if (envRING == null) {
                System.err.println("Error: RING environment variable required for method " + M);
                System.exit(1);
            }
        }

        if (M.equals("R")) {
            if ((RING <= 0) || ((RING & (RING - 1)) != 0)) {
                System.err.println("Error: RING capacity must be a power of two for R.");
                System.exit(1);
            }
        }

        if (!M.equals("B") && !M.equals("A") && !M.equals("R") &&
            !M.equals("Qbl") && !M.equals("Qul") && !M.equals("Quf")) {
            System.err.println("Error: Unsupported method " + M);
            System.exit(1);
        }

        boolean isTest = S.equals("test");
        int warmupRuns = 0;
        int realRuns = 1;

        if (!isTest) {
            String[] parts = S.split("/");
            if (parts.length != 2) {
                System.err.println("Error: Invalid suite parameter format: " + S);
                System.exit(1);
            }
            warmupRuns = Integer.parseInt(parts[0]);
            realRuns = Integer.parseInt(parts[1]);
        }

        // Run warmups
        for (int r = 0; r < warmupRuns; r++) {
            runOnce(N, K, I, P, T, M, ALLOC, RING, false, SEED + r);
        }

        if (!isTest && warmupRuns > 0) {
            System.out.println("---");
        }

        // Run measured runs
        for (int r = 0; r < realRuns; r++) {
            runOnce(N, K, I, P, T, M, ALLOC, RING, isTest, SEED + warmupRuns + r);
        }
    }

    static void runOnce(long N, int K, int I, int P, int T, String M, String ALLOC, int RING, boolean isTest, long seed) {
        if (M.equals("B")) {
            runBaseline(N, K, I, ALLOC, isTest, seed);
            return;
        }

        PaddedAtomicLong inClaim = new PaddedAtomicLong(0);
        final double[] finalSum = new double[1];
        final long[] durationNs = new long[1];

        // Structures based on method
        final Object[] pbuf;
        final AtomicIntegerArray pReady;
        final double[] cval;
        final AtomicIntegerArray cReady;

        final VyukovRingFloat inRingFloat;
        final VyukovRingDouble outRingDouble;

        final ArrayBlockingQueue<float[]> qblIn;
        final ArrayBlockingQueue<Double> qblOut;

        final LinkedBlockingQueue<float[]> qulIn;
        final LinkedBlockingQueue<Double> qulOut;

        final ConcurrentLinkedQueue<float[]> qufIn;
        final ConcurrentLinkedQueue<Double> qufOut;

        final float[][] pool;

        if (ALLOC.equals("pool")) {
            pool = new float[(int) N][K];
        } else {
            pool = null;
        }

        if (M.equals("A")) {
            pbuf = new Object[(int) N];
            pReady = new AtomicIntegerArray((int) N);
            cval = new double[(int) N];
            cReady = new AtomicIntegerArray((int) N);
            if (ALLOC.equals("pool")) {
                for (int i = 0; i < (int) N; i++) {
                    pbuf[i] = pool[i];
                }
            }
        } else {
            pbuf = null;
            pReady = null;
            cval = null;
            cReady = null;
        }

        if (M.equals("R")) {
            inRingFloat = new VyukovRingFloat(RING);
            outRingDouble = new VyukovRingDouble(RING);
        } else {
            inRingFloat = null;
            outRingDouble = null;
        }

        if (M.equals("Qbl")) {
            qblIn = new ArrayBlockingQueue<>(RING);
            qblOut = new ArrayBlockingQueue<>(RING);
        } else {
            qblIn = null;
            qblOut = null;
        }

        if (M.equals("Qul")) {
            qulIn = new LinkedBlockingQueue<>();
            qulOut = new LinkedBlockingQueue<>();
        } else {
            qulIn = null;
            qulOut = null;
        }

        if (M.equals("Quf")) {
            qufIn = new ConcurrentLinkedQueue<>();
            qufOut = new ConcurrentLinkedQueue<>();
        } else {
            qufIn = null;
            qufOut = null;
        }

        // Consumer thread
        Thread consumerThread = new Thread(() -> {
            double sum = 0.0;
            if (M.equals("A")) {
                for (int idx = 0; idx < (int) N; idx++) {
                    while (cReady.get(idx) == 0) {
                        Thread.onSpinWait();
                    }
                    sum += cval[idx];
                }
            } else if (M.equals("R")) {
                for (long c = 0; c < N; c++) {
                    Double val;
                    while ((val = outRingDouble.dequeue()) == null) {
                        Thread.onSpinWait();
                    }
                    sum += val;
                }
            } else if (M.equals("Qbl")) {
                for (long c = 0; c < N; c++) {
                    try {
                        sum += qblOut.take();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            } else if (M.equals("Qul")) {
                for (long c = 0; c < N; c++) {
                    try {
                        sum += qulOut.take();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            } else if (M.equals("Quf")) {
                for (long c = 0; c < N; c++) {
                    Double val;
                    while ((val = qufOut.poll()) == null) {
                        Thread.onSpinWait();
                    }
                    sum += val;
                }
            }
            long end = System.nanoTime();
            finalSum[0] = sum;
            durationNs[0] = end;
        });

        // Transformer threads
        Thread[] transformerThreads = new Thread[T];
        for (int t = 0; t < T; t++) {
            transformerThreads[t] = new Thread(() -> {
                while (true) {
                    long idx = inClaim.getAndIncrement();
                    if (idx >= N) break;

                    if (M.equals("A")) {
                        while (pReady.get((int) idx) == 0) {
                            Thread.onSpinWait();
                        }
                        double out = transform((float[]) pbuf[(int) idx], I);
                        cval[(int) idx] = out;
                        cReady.set((int) idx, 1);
                    } else if (M.equals("R")) {
                        float[] inMsg;
                        while ((inMsg = inRingFloat.dequeue()) == null) {
                            Thread.onSpinWait();
                        }
                        double out = transform(inMsg, I);
                        while (!outRingDouble.enqueue(out)) {
                            Thread.onSpinWait();
                        }
                    } else if (M.equals("Qbl")) {
                        try {
                            float[] inMsg = qblIn.take();
                            double out = transform(inMsg, I);
                            qblOut.put(out);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    } else if (M.equals("Qul")) {
                        try {
                            float[] inMsg = qulIn.take();
                            double out = transform(inMsg, I);
                            qulOut.put(out);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    } else if (M.equals("Quf")) {
                        float[] inMsg;
                        while ((inMsg = qufIn.poll()) == null) {
                            Thread.onSpinWait();
                        }
                        double out = transform(inMsg, I);
                        qufOut.add(out);
                    }
                }
            });
        }

        // Consumer and Transformers started outside timed region
        consumerThread.start();
        for (Thread t : transformerThreads) {
            t.start();
        }

        long start = System.nanoTime();

        // Producer threads
        Thread[] producerThreads = new Thread[P];
        for (int p = 0; p < P; p++) {
            final int pIndex = p;
            final int startId = (int) (pIndex * N / P);
            final int endId = (int) ((pIndex + 1) * N / P);

            producerThreads[p] = new Thread(() -> {
                for (int id = startId; id < endId; id++) {
                    float[] b = ALLOC.equals("pool") ? pool[id] : new float[K];
                    fillMessage(b, id, isTest, seed, pIndex);

                    if (M.equals("A")) {
                        if (!ALLOC.equals("pool")) {
                            pbuf[id] = b;
                        }
                        pReady.set(id, 1);
                    } else if (M.equals("R")) {
                        while (!inRingFloat.enqueue(b)) {
                            Thread.onSpinWait();
                        }
                    } else if (M.equals("Qbl")) {
                        try {
                            qblIn.put(b);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    } else if (M.equals("Qul")) {
                        try {
                            qulIn.put(b);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    } else if (M.equals("Quf")) {
                        qufIn.add(b);
                    }
                }
            });
            producerThreads[p].start();
        }

        // Wait for all threads to complete
        try {
            for (Thread p : producerThreads) {
                p.join();
            }
            for (Thread t : transformerThreads) {
                t.join();
            }
            consumerThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        long elapsed = durationNs[0] - start;
        System.out.printf("RESULT %d %.6f%n", elapsed, finalSum[0]);
    }

    static void runBaseline(long N, int K, int I, String ALLOC, boolean isTest, long seed) {
        float[][] pool = null;
        if (ALLOC.equals("pool")) {
            pool = new float[(int) N][K];
        }

        long start = System.nanoTime();
        double sum = 0.0;
        for (int id = 0; id < (int) N; id++) {
            float[] b = ALLOC.equals("pool") ? pool[id] : new float[K];
            fillMessage(b, id, isTest, seed, 0);
            sum += transform(b, I);
        }
        long end = System.nanoTime();
        long elapsed = end - start;
        System.out.printf("RESULT %d %.6f%n", elapsed, sum);
    }
}
