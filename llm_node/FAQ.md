# AxonFx - Frequently Asked Questions

## What is AxonFx?

AxonFx is a simulated cross-border payment system. It models the same
approach used by real remittance providers like Wise: instead of
physically moving money across a border for every transfer, it holds
pools of each currency and pays recipients out of the pool in their
currency, netting flows internally instead of wiring money abroad every
time.

## What currencies does it support?

Five: US Dollar (USD), Indian Rupee (INR), British Pound (GBP), Euro
(EUR), and Japanese Yen (JPY).

## How does a transfer actually work?

1. The sender's money is added to AxonFx's pool for that currency.
2. If the destination currency's pool already has enough to cover the
   payout, the recipient is paid straight from that pool - no
   conversion needed.
3. If the pool doesn't have enough, a matching engine looks at every
   other currency pool's available surplus and the current exchange
   rate, and greedily fills the shortfall starting with the best
   available rate, spilling into the next-best source if needed.
4. If there still isn't enough surplus anywhere, the remainder is
   covered by directly converting more of the sender's own currency.

## Why would a transfer use more than one currency pool?

Because using surplus sitting in another currency's pool, at a good
rate, is usually better than converting the sender's money directly.
This "netting" is the same idea real remittance companies use to avoid
moving money across an actual border for every single transfer.

## Is the exchange rate fixed or does it change?

It's simulated to behave like a live market rate - it drifts slightly
(a small fraction of a percent) from one moment to the next, deterministically,
so it looks and feels like a real feed without needing an external one.
Once a transfer locks in a rate, that rate is what the recipient is
paid at, regardless of what the rate does afterward.

## Does AxonFx charge a fee?

Yes - customers are quoted a rate that includes a small fee (under 1%)
built into the exchange rate itself, rather than a separate line-item
charge. The difference between that quoted rate and the true market
rate is the company's margin on the transfer.

## How is the system actually built?

Three application-server nodes run a hand-implemented Raft consensus
protocol (leader election, log replication, and the safety rules that
keep replicas consistent) so the ledger is replicated and stays
consistent even if one of the three nodes crashes. A concurrent-client
layer verifies the system holds up under many simultaneous requests,
including exact duplicate requests, without double-charging anyone.

## What happens if a server crashes mid-transfer?

The remaining two nodes detect the failure, elect a new leader among
themselves within a couple of seconds, and keep accepting transfers -
two of three nodes is still a majority. Nothing already committed is
ever lost. If the crashed node restarts, it recovers its exact state by
replaying its own saved history, with no manual intervention needed.

## Can I ask this assistant to send money?

No - this assistant only answers questions about how AxonFx works,
using this FAQ as its only source of information. It cannot submit a
transfer, and it cannot look up your real balance or transfer history,
since it isn't connected to the cluster at all. To actually send money
or check real balances, use the command-line client directly, for
example:

    uv run -m client.cli --node 127.0.0.1:7002 transfer USD INR 10000 --to "Priya Sharma" --account HDFC-1
    uv run -m client.cli --node 127.0.0.1:7002 balances

## What if my question isn't answered here?

This assistant only knows what's written in this file - it won't guess
at an answer it isn't confident about. If your question isn't covered
above, it should say so honestly rather than making something up.
