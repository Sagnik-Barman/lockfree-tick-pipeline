#pragma once
#include <array>
#include <cstdint>

#include "Packet.hpp"

// Stand-in for the cheap, per-message bookkeeping a real feed handler does
// before an update is usable by a strategy or matching engine (see
// limit-order-book-sim/ elsewhere in this repo for the *actual*
// price-time-priority book -- this project deliberately doesn't
// reimplement that; the point here is the pipeline it would sit behind).
// Kept O(1), allocation-free, and branch-light so it doesn't distort the
// latency numbers this project is actually trying to measure.
class BookUpdater {
public:
    static constexpr std::size_t kNumSymbols = 64;

    BookUpdater() { best_ask_.fill(kNoPrice); }

    // Returns true if this packet improved the best price on its side for
    // its symbol -- a cheap, realistic per-tick signal to compute.
    bool apply(const MarketDataPacket &pkt) {
        const std::size_t idx = pkt.symbol_id % kNumSymbols;
        if (pkt.side == 0) {
            const bool improved = pkt.price_ticks > best_bid_[idx];
            if (improved) best_bid_[idx] = pkt.price_ticks;
            return improved;
        } else {
            const bool improved = best_ask_[idx] == kNoPrice || pkt.price_ticks < best_ask_[idx];
            if (improved) best_ask_[idx] = pkt.price_ticks;
            return improved;
        }
    }

private:
    static constexpr std::int64_t kNoPrice = INT64_MAX;
    std::array<std::int64_t, kNumSymbols> best_bid_{};
    std::array<std::int64_t, kNumSymbols> best_ask_{};
};
