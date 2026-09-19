import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from axonfx_node.routing import plan_fills


def test_no_shortfall_returns_nothing():
    fills, remaining = plan_fills("INR", 0, "USD", {"USD": 100, "INR": 0}, {})
    assert fills == []
    assert remaining == 0


def test_prefers_best_rate_intermediate_over_direct_source():
    # GBP->INR is a much better rate than USD->INR here, and we have GBP
    # surplus sitting around - the engine should use GBP first.
    balances = {"USD": 10_000, "GBP": 5_000, "INR": 0}
    cross = {
        ("USD", "INR"): 80.0,
        ("GBP", "INR"): 200.0,  # deliberately great rate
    }
    fills, remaining = plan_fills("INR", 100_000, "USD", balances, cross)
    assert remaining == 0
    assert fills[0].via_currency == "GBP"
    assert fills[0].used_pool_surplus is True
    # GBP pool only had 5000 * 200 = 1,000,000 capacity - more than enough
    # to cover the full 100,000 alone in this case.
    assert len(fills) == 1
    assert abs(fills[0].produced - 100_000) < 1e-6


def test_spills_into_source_currency_when_pool_insufficient():
    balances = {"USD": 10_000, "GBP": 100, "INR": 0}  # tiny GBP surplus
    cross = {
        ("USD", "INR"): 80.0,
        ("GBP", "INR"): 200.0,
    }
    fills, remaining = plan_fills("INR", 100_000, "USD", balances, cross)
    assert remaining == 0
    # GBP fills first (best rate) but only covers 100*200 = 20,000
    assert fills[0].via_currency == "GBP"
    assert abs(fills[0].produced - 20_000) < 1e-6
    # the rest (80,000) spills into the source currency fallback
    assert fills[1].via_currency == "USD"
    assert fills[1].used_pool_surplus is False
    assert abs(fills[1].produced - 80_000) < 1e-6


def test_always_fully_covers_via_unlimited_source_fallback():
    balances = {"USD": 1, "INR": 0}  # basically no surplus anywhere
    cross = {("USD", "INR"): 80.0}
    fills, remaining = plan_fills("INR", 500_000, "USD", balances, cross)
    assert remaining == 0
    assert sum(f.produced for f in fills) == 500_000


if __name__ == "__main__":
    test_no_shortfall_returns_nothing()
    test_prefers_best_rate_intermediate_over_direct_source()
    test_spills_into_source_currency_when_pool_insufficient()
    test_always_fully_covers_via_unlimited_source_fallback()
    print("all routing tests passed")
