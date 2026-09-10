#pragma once
#include <algorithm>
#include <cstdint>
#include <vector>

// Collects per-message end-to-end latencies (nanoseconds) on the consumer
// side and reduces them to the percentiles that actually matter for a
// latency-critical system -- mean and p50 are close to meaningless on their
// own here; p99/p99.9 and max are what tell you whether the system is safe
// to put real capital behind, because tail latency is where a matching
// engine misses its window.
class LatencyStats {
public:
    void record(std::uint64_t ns) { samples_.push_back(ns); }

    struct Summary {
        std::size_t n = 0;
        double mean = 0.0;
        double p50 = 0.0;
        double p90 = 0.0;
        double p99 = 0.0;
        double p999 = 0.0;
        double max = 0.0;
    };

    Summary summarize() const {
        Summary s;
        s.n = samples_.size();
        if (s.n == 0) return s;

        std::vector<std::uint64_t> sorted = samples_;
        std::sort(sorted.begin(), sorted.end());

        auto pct = [&](double p) -> double {
            std::size_t idx = static_cast<std::size_t>(p * static_cast<double>(sorted.size() - 1));
            return static_cast<double>(sorted[idx]);
        };

        s.p50 = pct(0.50);
        s.p90 = pct(0.90);
        s.p99 = pct(0.99);
        s.p999 = pct(0.999);
        s.max = static_cast<double>(sorted.back());

        double sum = 0.0;
        for (auto v : samples_) sum += static_cast<double>(v);
        s.mean = sum / static_cast<double>(s.n);
        return s;
    }

    const std::vector<std::uint64_t> &raw() const { return samples_; }

private:
    std::vector<std::uint64_t> samples_;
};
