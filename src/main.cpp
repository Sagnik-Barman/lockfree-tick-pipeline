// Lock-free SPSC ring buffer vs. mutex + deque: end-to-end message latency.
//
//   tick_pipeline [n_packets] [rate_per_sec] [trials] [producer_core] [consumer_core]
//
//   n_packets      messages per trial (default 200000)
//   rate_per_sec   producer send rate (default 1000000). 0 = unpaced flood, which
//                  measures how fast a backlog drains, not hand-off latency.
//   trials         trials per queue, run alternately (default 5); the report is the
//                  median of each statistic across trials
//   producer_core, consumer_core
//                  logical CPUs to pin the two threads to (default 2 and 4 when the
//                  machine has at least 6; -1 = do not pin). Pick two different
//                  physical cores: on most machines logical CPUs 2k and 2k+1 are
//                  hyper-threads of the same core.
//
// Fair comparison, on purpose:
//   * both queues hold at most the same number of messages (4095), so neither can
//     hide a backlog the other is forced to refuse;
//   * the producer is paced to a fixed schedule, so latency means "time from send to
//     receive", not "time spent waiting behind a pile of earlier messages";
//   * the first 10,000 messages of each trial are warm-up and are not counted;
//   * the same seeded packet stream is used for every trial.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

#if defined(_WIN32)
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#elif defined(__linux__)
#include <pthread.h>
#include <sched.h>
#endif

#include "BookUpdater.hpp"
#include "LatencyStats.hpp"
#include "MutexQueue.hpp"
#include "Packet.hpp"
#include "SpscRingBuffer.hpp"

namespace {

using Clock = std::chrono::steady_clock;
constexpr std::size_t kRingCapacity = 4096;           // ring holds kRingCapacity - 1 items
constexpr std::size_t kQueueBound = kRingCapacity - 1;
constexpr std::size_t kWarmup = 10000;

std::uint64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count();
}

bool pin_current_thread(int core) {
    if (core < 0) return false;
#if defined(_WIN32)
    return SetThreadAffinityMask(GetCurrentThread(), static_cast<DWORD_PTR>(1) << core) != 0;
#elif defined(__linux__)
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(core, &set);
    return pthread_setaffinity_np(pthread_self(), sizeof(set), &set) == 0;
#else
    return false;
#endif
}

std::vector<MarketDataPacket> generate_packets(std::size_t n, unsigned seed) {
    std::mt19937_64 rng(seed);
    std::uniform_int_distribution<std::uint32_t> symbol_dist(0, 63);
    std::uniform_int_distribution<std::int64_t> price_dist(9900, 10100);
    std::uniform_int_distribution<int> qty_dist(1, 500);
    std::uniform_int_distribution<int> side_dist(0, 1);
    std::vector<MarketDataPacket> packets(n);
    for (std::size_t i = 0; i < n; ++i) {
        packets[i].seq = i;
        packets[i].symbol_id = symbol_dist(rng);
        packets[i].price_ticks = price_dist(rng);
        packets[i].qty = qty_dist(rng);
        packets[i].side = static_cast<std::uint8_t>(side_dist(rng));
    }
    return packets;
}

struct LockFreeQueue {
    SpscRingBuffer<MarketDataPacket, kRingCapacity> ring;
    bool push(const MarketDataPacket &p) { return ring.try_push(p); }
    bool pop(MarketDataPacket &p) { return ring.try_pop(p); }
    static constexpr const char *name = "lock_free_spsc";
};

struct LockedQueue {
    MutexQueue<MarketDataPacket> q{kQueueBound};
    bool push(const MarketDataPacket &p) { return q.try_push(p); }
    bool pop(MarketDataPacket &p) { return q.try_pop(p); }
    static constexpr const char *name = "mutex_deque";
};

struct Config {
    std::size_t n = 200000;
    double rate = 1e6;
    int trials = 5;
    int producer_core = -1;
    int consumer_core = -1;
};

struct Trial {
    LatencyStats::Summary s;
    double achieved_rate = 0.0;
    std::size_t late_sends = 0;   // sends that started after their scheduled time
    std::size_t full_spins = 0;   // push attempts that found the queue full
    std::vector<std::uint64_t> raw;
    bool pinned = false;
};

template <typename Q>
Trial run_trial(const std::vector<MarketDataPacket> &packets, const Config &cfg) {
    auto q = std::make_unique<Q>();
    std::vector<std::uint64_t> lat(packets.size());
    std::atomic<bool> consumer_ready{false};
    std::atomic<bool> pinned_ok{true};
    Trial t;

    std::thread consumer([&] {
        if (cfg.consumer_core >= 0 && !pin_current_thread(cfg.consumer_core)) pinned_ok = false;
        BookUpdater book;
        MarketDataPacket pkt;
        std::size_t i = 0;
        consumer_ready.store(true, std::memory_order_release);
        while (i < packets.size()) {
            if (q->pop(pkt)) {
                lat[i++] = now_ns() - pkt.send_time_ns;
                book.apply(pkt);
            }
        }
    });

    std::uint64_t t_start = 0, t_end = 0;
    std::thread producer([&] {
        if (cfg.producer_core >= 0 && !pin_current_thread(cfg.producer_core)) pinned_ok = false;
        while (!consumer_ready.load(std::memory_order_acquire)) {
        }
        const double interval = cfg.rate > 0 ? 1e9 / cfg.rate : 0.0;
        const std::uint64_t t0 = now_ns() + 1'000'000;  // start 1 ms from now
        t_start = t0;
        for (std::size_t i = 0; i < packets.size(); ++i) {
            MarketDataPacket pkt = packets[i];
            if (interval > 0) {
                const std::uint64_t due = t0 + static_cast<std::uint64_t>(interval * static_cast<double>(i));
                std::uint64_t now = now_ns();
                if (now > due + 1000) ++t.late_sends;  // more than 1 us behind schedule
                while (now < due) now = now_ns();
            }
            pkt.send_time_ns = now_ns();
            while (!q->push(pkt)) ++t.full_spins;   // time spent full counts as latency
        }
        t_end = now_ns();
    });

    producer.join();
    consumer.join();

    LatencyStats stats;
    for (std::size_t i = std::min(kWarmup, lat.size()); i < lat.size(); ++i) stats.record(lat[i]);
    t.s = stats.summarize();
    t.raw = stats.raw();
    t.achieved_rate = static_cast<double>(packets.size()) / (static_cast<double>(t_end - t_start) * 1e-9);
    t.pinned = cfg.producer_core >= 0 && cfg.consumer_core >= 0 && pinned_ok;
    return t;
}

double median(std::vector<double> v) {
    std::sort(v.begin(), v.end());
    return v.empty() ? 0.0 : v[v.size() / 2];
}

struct Report {
    std::string name;
    double p50, p90, p99, p999, max, rate;
    std::size_t late, full;
};

Report summarize(const std::string &name, const std::vector<Trial> &trials) {
    auto med = [&](auto f) {
        std::vector<double> v;
        for (const auto &t : trials) v.push_back(f(t));
        return median(v);
    };
    Report r{name,
             med([](const Trial &t) { return t.s.p50; }),
             med([](const Trial &t) { return t.s.p90; }),
             med([](const Trial &t) { return t.s.p99; }),
             med([](const Trial &t) { return t.s.p999; }),
             med([](const Trial &t) { return t.s.max; }),
             med([](const Trial &t) { return t.achieved_rate; }),
             0, 0};
    for (const auto &t : trials) {
        r.late += t.late_sends;
        r.full += t.full_spins;
    }
    return r;
}

}  // namespace

int main(int argc, char **argv) {
    Config cfg;
    const unsigned hw = std::thread::hardware_concurrency();
    if (hw >= 6) {
        cfg.producer_core = 2;
        cfg.consumer_core = 4;
    }
    if (argc > 1) cfg.n = std::stoul(argv[1]);
    if (argc > 2) cfg.rate = std::stod(argv[2]);
    if (argc > 3) cfg.trials = std::max(1, std::stoi(argv[3]));
    if (argc > 4) cfg.producer_core = std::stoi(argv[4]);
    if (argc > 5) cfg.consumer_core = std::stoi(argv[5]);
    cfg.n = std::max(cfg.n, kWarmup * 2);

    std::cout << "Packets per trial: " << cfg.n << " (first " << kWarmup << " are warm-up)\n"
              << "Send rate:         " << (cfg.rate > 0 ? std::to_string(static_cast<long long>(cfg.rate)) + " msg/s"
                                                        : std::string("unpaced (flood)")) << "\n"
              << "Trials per queue:  " << cfg.trials << ", alternating\n"
              << "Queue bound:       " << kQueueBound << " messages for both queues\n"
              << "Logical CPUs:      " << hw << ", pinning producer to " << cfg.producer_core
              << " and consumer to " << cfg.consumer_core << " (-1 = not pinned)\n";

    const auto packets = generate_packets(cfg.n, /*seed=*/42);
    std::vector<Trial> lf, mx;
    for (int k = 0; k < cfg.trials; ++k) {
        lf.push_back(run_trial<LockFreeQueue>(packets, cfg));
        mx.push_back(run_trial<LockedQueue>(packets, cfg));
        std::cout << "  trial " << k + 1 << ": lock-free p99 " << lf.back().s.p99 << " ns, mutex p99 "
                  << mx.back().s.p99 << " ns\n";
    }
    if (cfg.producer_core >= 0 && !lf.back().pinned) std::cout << "WARNING: thread pinning failed\n";

    const Report a = summarize(LockFreeQueue::name, lf), b = summarize(LockedQueue::name, mx);
    std::cout << std::fixed << std::setprecision(0)
              << "\nMedian across trials (ns)\n"
              << "  queue                p50      p90      p99    p99.9       max   achieved msg/s\n";
    for (const auto &r : {a, b}) {
        std::cout << "  " << std::left << std::setw(16) << r.name << std::right << std::setw(8) << r.p50
                  << std::setw(9) << r.p90 << std::setw(9) << r.p99 << std::setw(9) << r.p999 << std::setw(10)
                  << r.max << std::setw(15) << r.rate << "\n";
    }
    std::cout << std::setprecision(2) << "\nmutex / lock-free:  p50 " << b.p50 / std::max(1.0, a.p50) << "x   p99 "
              << b.p99 / std::max(1.0, a.p99) << "x   p99.9 " << b.p999 / std::max(1.0, a.p999) << "x\n";
    if (cfg.rate > 0 && (a.late + b.late) > cfg.n / 100)
        std::cout << "NOTE: the producer fell behind schedule on " << a.late + b.late
                  << " sends; the requested rate may be too high for this machine.\n";
    std::cout << "Queue-full push attempts: lock-free " << a.full << ", mutex " << b.full << "\n";

    std::ofstream raw("output/latencies.csv");
    raw << "pipeline,seq,latency_ns\n";
    for (const auto *t : {&lf.back(), &mx.back()}) {
        const char *name = (t == &lf.back()) ? LockFreeQueue::name : LockedQueue::name;
        for (std::size_t i = 0; i < t->raw.size(); ++i) raw << name << "," << i << "," << t->raw[i] << "\n";
    }
    std::ofstream summ("output/summary.csv");
    summ << "pipeline,trial,p50,p90,p99,p999,max,achieved_rate\n";
    for (std::size_t k = 0; k < lf.size(); ++k) {
        for (const auto &[name, t] : {std::pair{LockFreeQueue::name, &lf[k]}, std::pair{LockedQueue::name, &mx[k]}})
            summ << name << "," << k + 1 << "," << t->s.p50 << "," << t->s.p90 << "," << t->s.p99 << "," << t->s.p999
                 << "," << t->s.max << "," << t->achieved_rate << "\n";
    }
    std::cout << "\nWrote output/latencies.csv (last trial, raw) and output/summary.csv (every trial)\n"
              << "Run scripts/plot_latency.py to render output/latency_comparison.png\n";
    return 0;
}
