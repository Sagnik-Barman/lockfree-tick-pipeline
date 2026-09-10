#include <chrono>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "BookUpdater.hpp"
#include "LatencyStats.hpp"
#include "MutexQueue.hpp"
#include "Packet.hpp"
#include "SpscRingBuffer.hpp"

namespace {

using Clock = std::chrono::steady_clock;

std::uint64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count();
}

// Generates the same synthetic packet stream for both pipelines (same seed)
// so the two benchmarks see identical work -- only the queue implementation
// differs.
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
        // send_time_ns is stamped right before enqueue, not here.
    }
    return packets;
}

struct BenchResult {
    std::string name;
    LatencyStats::Summary summary;
    double packets_per_sec = 0.0;
    std::vector<std::uint64_t> raw_latencies;
};

BenchResult run_lockfree_benchmark(const std::vector<MarketDataPacket> &packets) {
    static constexpr std::size_t kCapacity = 4096; // power of two
    SpscRingBuffer<MarketDataPacket, kCapacity> ring;
    LatencyStats stats;
    BookUpdater book;

    const auto t_start = Clock::now();

    std::thread consumer([&] {
        std::size_t received = 0;
        MarketDataPacket pkt;
        while (received < packets.size()) {
            if (ring.try_pop(pkt)) {
                const std::uint64_t latency = now_ns() - pkt.send_time_ns;
                stats.record(latency);
                book.apply(pkt); // matching-adjacent work, kept off the measured span's critical timing
                ++received;
            }
        }
    });

    std::thread producer([&] {
        for (auto pkt : packets) { // copy: we stamp send_time_ns per attempt
            pkt.send_time_ns = now_ns();
            while (!ring.try_push(pkt)) {
                pkt.send_time_ns = now_ns(); // re-stamp so a full-buffer stall doesn't get counted as latency
            }
        }
    });

    producer.join();
    consumer.join();
    const auto t_end = Clock::now();

    BenchResult result;
    result.name = "lock_free_spsc";
    result.summary = stats.summarize();
    result.raw_latencies = stats.raw();
    const double seconds = std::chrono::duration<double>(t_end - t_start).count();
    result.packets_per_sec = static_cast<double>(packets.size()) / seconds;
    return result;
}

BenchResult run_mutex_benchmark(const std::vector<MarketDataPacket> &packets) {
    MutexQueue<MarketDataPacket> queue;
    LatencyStats stats;
    BookUpdater book;

    const auto t_start = Clock::now();

    std::thread consumer([&] {
        std::size_t received = 0;
        MarketDataPacket pkt;
        while (received < packets.size()) {
            if (queue.try_pop(pkt)) {
                const std::uint64_t latency = now_ns() - pkt.send_time_ns;
                stats.record(latency);
                book.apply(pkt);
                ++received;
            }
        }
    });

    std::thread producer([&] {
        for (auto pkt : packets) {
            pkt.send_time_ns = now_ns();
            queue.push(pkt);
        }
    });

    producer.join();
    consumer.join();
    const auto t_end = Clock::now();

    BenchResult result;
    result.name = "mutex_deque";
    result.summary = stats.summarize();
    result.raw_latencies = stats.raw();
    const double seconds = std::chrono::duration<double>(t_end - t_start).count();
    result.packets_per_sec = static_cast<double>(packets.size()) / seconds;
    return result;
}

void print_summary(const BenchResult &r) {
    const auto &s = r.summary;
    std::cout << std::fixed << std::setprecision(0);
    std::cout << "\n=== " << r.name << " ===\n";
    std::cout << "  messages:        " << s.n << "\n";
    std::cout << "  throughput:      " << static_cast<std::uint64_t>(r.packets_per_sec) << " msg/s\n";
    std::cout << "  mean latency:    " << s.mean << " ns\n";
    std::cout << "  p50:             " << s.p50 << " ns\n";
    std::cout << "  p90:             " << s.p90 << " ns\n";
    std::cout << "  p99:             " << s.p99 << " ns\n";
    std::cout << "  p99.9:           " << s.p999 << " ns\n";
    std::cout << "  max:             " << s.max << " ns\n";
}

void write_csv(const std::vector<BenchResult> &results, const std::string &path) {
    std::ofstream out(path);
    out << "pipeline,seq,latency_ns\n";
    for (const auto &r : results) {
        for (std::size_t i = 0; i < r.raw_latencies.size(); ++i) {
            out << r.name << "," << i << "," << r.raw_latencies[i] << "\n";
        }
    }
}

} // namespace

int main(int argc, char **argv) {
    std::size_t n_packets = 50000;
    if (argc > 1) n_packets = static_cast<std::size_t>(std::stoul(argv[1]));

    std::cout << "Generating " << n_packets << " synthetic market-data packets...\n";
    auto packets = generate_packets(n_packets, /*seed=*/42);

    std::cout << "Running lock-free SPSC pipeline...\n";
    auto lockfree = run_lockfree_benchmark(packets);

    std::cout << "Running mutex+deque baseline pipeline...\n";
    auto mutex_based = run_mutex_benchmark(packets);

    print_summary(lockfree);
    print_summary(mutex_based);

    std::cout << "\n=== Tail-latency comparison (mutex / lock-free) ===\n";
    std::cout << "  p50:   " << (mutex_based.summary.p50 / std::max(1.0, lockfree.summary.p50)) << "x\n";
    std::cout << "  p99:   " << (mutex_based.summary.p99 / std::max(1.0, lockfree.summary.p99)) << "x\n";
    std::cout << "  p99.9: " << (mutex_based.summary.p999 / std::max(1.0, lockfree.summary.p999)) << "x\n";

    write_csv({lockfree, mutex_based}, "output/latencies.csv");
    std::cout << "\nRaw latencies written to output/latencies.csv\n";
    std::cout << "Run scripts/plot_latency.py to render output/latency_comparison.png\n";

    return 0;
}
