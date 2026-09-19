"""
The matching / netting engine.

AxonFx doesn't run a continuous double-auction order book - there's no
independent buyer/seller pricing to match. What it actually needs to
decide, every time a payout can't be fully covered by the destination
pool alone, is: *which currencies should fund the shortfall, and in what
order, to get the best effective rate* - called the "intermediate
routing", example: (USD -> GBP -> INR beating USD -> INR
directly when GBP happens to be cheap to convert right now).

That is a fractional knapsack problem: each currency pool is a "source"
with a fixed capacity (its balance) and a fixed unit rate (its cross rate
to the destination currency); we need a fixed total amount of the
destination currency; and greedily filling from the best rate down is
*provably* optimal for fractional knapsack. So instead of a full
order-book/price-time-priority matcher, one small greedy pass gives the
mathematically best answer for this exact problem shape.

"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Fill:
    via_currency: str
    drained: float     # units of via_currency consumed from that pool
    produced: float     # units of dest_currency produced
    rate: float
    used_pool_surplus: bool  # False => this portion needed a fresh forex-partner conversion


# Currencies whose entire existing pool balance is available to help fund a
# payout are capped at their balance. The currency the sender's money just
# arrived in is always allowed to go further than its nominal pool balance,
# because AxonFx can always call its forex partner to convert more of the
# money that's already sitting in that account (Scenario 2's "direct
# conversion" fallback). We model that as a very large cap rather than
# literal infinity so the arithmetic stays well-behaved.
UNLIMITED_CAP = 10 ** 15


def plan_fills(dest_currency, amount_needed, source_currency, balances, cross_rates):
    """
    dest_currency: str
    amount_needed: float - shortfall in dest_currency after using whatever
        was already free in the destination pool
    source_currency: str - the currency the sender's funds arrived in
    balances: dict[str, float] - current pool balances (already includes
        the sender's freshly-credited funds)
    cross_rates: dict[(str, str), float] - execution rate table, produced
        once by state_machine.RateSnapshot.cross_rate_table()

    Returns (fills: list[Fill], shortfall_remaining: float)
    shortfall_remaining should always be ~0 given the unlimited fallback,
    but is returned rather than assumed so callers can assert on it.
    """
    if amount_needed <= 1e-9:
        return [], 0.0

    candidates = []
    for ccy, bal in balances.items():
        if ccy == dest_currency:
            continue
        rate = cross_rates.get((ccy, dest_currency))
        if not rate or rate <= 0:
            continue
        is_source = ccy == source_currency
        cap = UNLIMITED_CAP if is_source else max(bal, 0.0)
        if cap <= 1e-9:
            continue
        candidates.append((ccy, cap, rate, is_source))

    # Best effective rate first - the greedy step that makes this optimal.
    candidates.sort(key=lambda c: c[2], reverse=True)

    remaining = amount_needed
    fills = []
    for ccy, cap, rate, is_source in candidates:
        if remaining <= 1e-9:
            break
        max_producible = cap * rate
        take_dest = min(remaining, max_producible)
        take_source_units = take_dest / rate
        fills.append(Fill(
            via_currency=ccy,
            drained=round(take_source_units, 6),
            produced=round(take_dest, 6),
            rate=rate,
            # True = funded via an intermediate currency's surplus balance
            # (Scenario 3, "intermediate routing"). False = a direct
            # conversion of the sender's own inbound funds (Scenario 2).
            used_pool_surplus=not is_source,
        ))
        remaining -= take_dest

    return fills, max(remaining, 0.0)
