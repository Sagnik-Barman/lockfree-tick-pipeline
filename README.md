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
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release      # on MSYS2/Windows add: -G "MinGW Makefiles"
cmake --build build
./build/tick_pipeline 200000 1000000 5 2 4         # packets, msg/s, trials, producer core, consumer core
python scripts/plot_latency.py                      # needs pandas + matplotlib
```

Or without CMake: `g++ -std=c++20 -O2 -Iinclude src/main.cpp -o tick_pipeline -pthread`.

## How the benchmark is made fair

The first version of this benchmark flattered the lock-free queue: the
producer sent as fast as it could, the ring buffer was bounded but the mutex
queue was not, so the mutex numbers (milliseconds) mostly measured how long a
message sat behind a growing backlog, not the cost of the lock. Now:

- **Both queues have the same bound** (4,095 messages). `MutexQueue::try_push`
  refuses when full, exactly like `SpscRingBuffer::try_push`.
- **The producer is paced** to a fixed schedule (default 1M messages/s), so
  latency is send-to-receive time. Rate `0` gives the old unpaced flood.
  If the producer cannot keep up with the schedule, the program says so.
- **Threads are pinned** to two logical CPUs (`SetThreadAffinityMask` on
  Windows, `pthread_setaffinity_np` on Linux). Use two different physical
  cores: logical CPUs 2k and 2k+1 are usually hyper-threads of one core.
- **Five alternating trials** per queue, 10,000 warm-up messages dropped from
  each, and the report is the median of each percentile across trials
  (`output/summary.csv` has every trial).

## Reading the output

`tick_pipeline` prints the median p50 / p90 / p99 / p99.9 / max latency per
queue and the mutex-to-lock-free ratio at p50 / p99 / p99.9.
`output/latencies.csv` has every sample from the last trial, and
`scripts/plot_latency.py` turns it into `output/latency_comparison.png`.

Expect the lock-free queue to win clearly at p50 (a hand-off is two atomic
operations with nothing to wait for). The tails are where machine noise shows
up: run on mains power, in a high-performance power mode, with other programs
closed, and check that the five trials agree before quoting a number.

## Known limitations (say these before you're asked)

- On Windows, `steady_clock` ticks in 100 ns steps, so latencies are
  quantised to 100 ns there; Linux gives nanosecond-resolution timestamps.
- The numbers checked into `output/` come from a shared 2-vCPU cloud sandbox
  and are not numbers to quote. Re-run on your own machine.
- The "market data" is synthetic random packets, not a real exchange binary
  protocol (ITCH, OUCH, FIX/FAST): decoding is assumed already done.
- Single producer, single consumer only. Fanning out to several consumers (a
  strategy thread and a risk thread, say) is the much harder MPMC problem.

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

- Extend to MPMC (multiple feed threads, e.g. one per exchange session) --
  this is where you actually need a CAS retry loop, and where the ABA
  problem becomes real.
- Feed this pipeline's output into `../limit-order-book-sim/`'s `OrderBook`
  instead of the toy `BookUpdater`, and measure tick-to-book latency against
  a real matching engine.
- Add a bounded-buffer backpressure policy (drop, or block) for when the
  consumer genuinely can't keep up, and measure what each does to tail
  latency under a burst.
