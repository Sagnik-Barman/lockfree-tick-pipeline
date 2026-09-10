# Lock-Free Tick-to-Book Pipeline (C++20)

A two-stage market-data pipeline -- a producer thread standing in for a feed
handler, a consumer thread doing cheap per-symbol book bookkeeping -- built
twice: once on a lock-free SPSC ring buffer, once on the "obvious" way
(`std::mutex` + `std::deque`). It benchmarks the two head-to-head on
end-to-end message latency, because the whole point of reaching for
lock-free code is a latency claim, and a latency claim you haven't measured
isn't one yet.

This is the project that answers the JD line "writing modern C++ 17/20 on
latency-critical paths -- order entry, market-data decoding, matching-adjacent
logic" directly, and gives you something concrete to say about "lock-free
concurrency" instead of just the phrase.

## What's actually being modeled

**`SpscRingBuffer<T, Capacity>`** (`include/SpscRingBuffer.hpp`). A classic
single-producer/single-consumer lock-free ring buffer -- the same shape as
Herb Sutter's "Lock-Free Queue" design and folly's `ProducerConsumerQueue`.
Because there is exactly one writer and one reader, each side only ever
*writes* its own index and only ever *reads* the other side's index, so a
pair of `std::atomic<size_t>` with acquire/release ordering is sufficient --
no compare-and-swap loop, no ABA problem, no hazard pointers. `head_` and
`tail_` are each pinned to their own cache line (`alignas(64)`) so the
producer and consumer never cause false sharing on the control state itself.

**`MutexQueue<T>`** (`include/MutexQueue.hpp`). The baseline: `std::mutex` +
`std::deque`, given the *same* polling shape (consumer busy-loops on
`try_pop`) rather than a `condition_variable`, so the benchmark isolates the
cost of the synchronization primitive itself, not a mix of that plus
wake-latency from blocking.

**`BookUpdater`** (`include/BookUpdater.hpp`). Stand-in for the cheap,
allocation-free per-tick bookkeeping a real feed handler does downstream of
decode -- O(1), branch-light, on purpose, so it doesn't distort the latency
numbers. (The actual price-time-priority order book lives in
`../limit-order-book-sim/` elsewhere in this repo; this project is deliberately
about the pipeline that would sit in front of it, not a second copy of it.)

**`LatencyStats`** (`include/LatencyStats.hpp`). Records one
enqueue-to-dequeue latency per message and reduces it to percentiles rather
than an average -- p50 tells you almost nothing about whether a
latency-sensitive system is safe to run; p99/p99.9/max are what tell you
that, because tail latency is where a real matching engine misses its
window.

## Building and running

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make
cd ..
./build/tick_pipeline 50000        # run both pipelines on 50k synthetic packets each
python scripts/plot_latency.py     # needs pandas + matplotlib
```

## Reading the output

`tick_pipeline` prints throughput and a latency percentile table for each
pipeline, then a tail-latency ratio (mutex / lock-free) at p50/p99/p99.9, and
writes every raw sample to `output/latencies.csv`.
`scripts/plot_latency.py` turns that into `output/latency_comparison.png`: a
percentile bar chart (log-scaled, since tails routinely differ by an order of
magnitude) and a latency-distribution histogram for both pipelines.

The pattern to expect: the lock-free pipeline's p50 should be dramatically
lower (single-digit microseconds, since there's genuinely nothing to wait
for), and it should hold its tail (p99/p99.9) far more tightly than the
mutex-based one, which is prone to occasional long waits whenever the
producer holds the lock at an inconvenient moment for the consumer.

## Known limitations (say these before you're asked)

- **This was built and benchmarked in a shared, 2-vCPU cloud sandbox with no
  thread pinning or CPU isolation** -- the absolute latency numbers in this
  repo's checked-in `output/` are inflated by container scheduling noise
  (you'll see occasional millisecond-scale outliers on *both* pipelines,
  which shouldn't happen on a dedicated machine) and are not numbers to put
  on a resume. **Re-run `./build/tick_pipeline` on your own machine** before
  quoting a number in an interview -- the relative comparison (lock-free
  beats mutex on tail latency, consistently) is the real result; the
  absolute nanosecond figures are not, until you've measured them on
  hardware you control.
- No CPU affinity / thread pinning (`pthread_setaffinity_np` or platform
  equivalent) -- a real low-latency system pins the producer and consumer to
  isolated cores specifically to get rid of the scheduling noise mentioned
  above. Worth adding as the first real extension.
- The "market data" is synthetic random packets, not a real exchange binary
  protocol (ITCH, OUCH, FIX/FAST) -- decoding is assumed already done; this
  project starts one stage downstream of that.
- Single producer, single consumer only. A real feed handler often needs to
  fan out to multiple consumers (a strategy thread and a risk thread, say),
  which is a fundamentally harder lock-free problem (MPMC) than what's
  implemented here -- see "extensions" below.

## What to be ready to explain in an interview

- **Why SPSC gets to skip everything that makes general lock-free queues
  hard.** With one writer and one reader, there's no contention on who gets
  to write an index -- each side owns exactly one atomic. MPMC needs
  compare-and-swap retry loops and has to solve the ABA problem; SPSC
  structurally can't hit it.
- **Why `head_`/`tail_` are cache-line-padded.** Without `alignas(64)`, the
  two atomics likely share a 64-byte cache line, and every push/pop would
  ping-pong that line between the producer's and consumer's core caches
  (false sharing) -- a correctness-neutral bug that silently costs most of
  the performance you built the lock-free structure to get.
- **Acquire/release, not sequential consistency.** `try_push`'s release
  store on `tail_` and `try_pop`'s acquire load of `tail_` form a
  synchronizes-with pair: the consumer is guaranteed to see the slot write
  that happened-before it, without needing the stronger (and slower)
  default `memory_order_seq_cst`.
- **Why a mutex isn't "wrong", just a different trade-off.** A blocking
  `condition_variable`-based queue would use far less CPU than either
  benchmark here (both busy-poll) at the cost of wake-up latency -- the
  right choice depends on whether you're optimizing for tail latency
  (lock-free/busy-poll) or for not burning a whole core (blocking).

## Extensions worth doing if you have more time

- Pin producer and consumer to specific cores and re-measure -- this is the
  single highest-value fix for getting numbers you can actually defend.
- Extend to MPMC (multiple feed threads, e.g. one per exchange session) --
  this is where you actually need a CAS retry loop, and where the ABA
  problem becomes real.
- Feed this pipeline's output into `../limit-order-book-sim/`'s `OrderBook`
  instead of the toy `BookUpdater`, and measure tick-to-book latency against
  a real matching engine.
- Add a bounded-buffer backpressure policy (drop, or block) for when the
  consumer genuinely can't keep up, and measure what each does to tail
  latency under a burst.
