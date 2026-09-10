#pragma once
#include <cstdint>

// A stand-in for a decoded market-data message (think: one ITCH/OUCH-style
// "add order" or "trade" message after the binary protocol has already been
// parsed). Fixed-size and trivially copyable on purpose -- a real feed
// handler never wants an allocation on this path.
struct MarketDataPacket {
    std::uint64_t seq = 0;            // monotonically increasing sequence number
    std::uint64_t send_time_ns = 0;   // producer-side timestamp, set at enqueue
    std::uint32_t symbol_id = 0;
    std::int64_t price_ticks = 0;
    std::int32_t qty = 0;
    std::uint8_t side = 0;            // 0 = buy, 1 = sell
};

static_assert(std::is_trivially_copyable_v<MarketDataPacket>,
              "packets must be trivially copyable -- no allocation on the hot path");
