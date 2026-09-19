import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from axonfx_node.state_machine import AxonFxState, RateSnapshot, encode_submit_transfer, STARTING_POOLS


def make_command(request_id, source, dest, amount, snapshot=None):
    snapshot = snapshot or RateSnapshot(taken_at=0.0, usd_rates=dict(
        USD=1.0, INR=1 / 83.0, GBP=1.27, EUR=1.09, JPY=1 / 149.0))
    return encode_submit_transfer(
        request_id=request_id, source_currency=source, dest_currency=dest, source_amount=amount,
        locked_rate=snapshot.locked_rate(source, dest), mid_rate=snapshot.mid_cross(source, dest),
        cross_rates=snapshot.cross_rate_table(), recipient_name="Test", recipient_account="ACC-1",
        decided_at=0.0)


def test_pool_covered_transfer_moves_exact_amounts():
    state = AxonFxState()
    cmd = make_command("r1", "USD", "INR", 10_000)
    result = state.apply(cmd)
    assert result["success"] is True
    assert result["scenario"] == "pool_covered"
    assert state.balances["USD"] == STARTING_POOLS["USD"] + 10_000
    assert state.balances["INR"] == round(STARTING_POOLS["INR"] - result["dest_amount_paid"], 2)


def test_idempotent_replay_does_not_double_apply():
    state = AxonFxState()
    cmd = make_command("dup", "USD", "INR", 5_000)
    first = state.apply(cmd)
    balance_after_first = dict(state.balances)
    second = state.apply(cmd)  # identical command, same request_id
    assert first["idempotent_replay"] is False
    assert second["idempotent_replay"] is True
    assert second["dest_amount_paid"] == first["dest_amount_paid"]
    assert state.balances == balance_after_first  # unchanged by the replay
    assert len(state.transfer_log) == 1  # no duplicate history entry


def test_deficit_triggers_routing_through_matching_engine():
    state = AxonFxState()
    # Big enough to blow through the INR pool, forcing routing.py to kick in.
    cmd = make_command("r-big", "USD", "INR", 40_000)
    result = state.apply(cmd)
    assert result["success"] is True
    assert result["scenario"] in ("routed_intermediate", "routed_intermediate+forex_fallback", "forex_direct")
    assert state.balances["INR"] <= 0.01  # pool fully drained toward the payout


def test_apply_is_a_pure_function_of_command_bytes():
    # Same command applied to two independent, freshly-constructed states
    # must produce byte-identical results - this is the property raft.py
    # relies on to keep every replica's ledger consistent.
    cmd = make_command("r-det", "GBP", "EUR", 777)
    state_a = AxonFxState()
    state_b = AxonFxState()
    result_a = state_a.apply(cmd)
    result_b = state_b.apply(cmd)
    assert result_a == result_b
    assert state_a.balances == state_b.balances


if __name__ == "__main__":
    test_pool_covered_transfer_moves_exact_amounts()
    test_idempotent_replay_does_not_double_apply()
    test_deficit_triggers_routing_through_matching_engine()
    test_apply_is_a_pure_function_of_command_bytes()
    print("all state machine tests passed")
