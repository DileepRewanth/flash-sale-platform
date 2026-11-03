# Runbook: live incident during a drop

All admin calls need `Authorization: Bearer $ADMIN_TOKEN`. Replace `$API` with the service address and `drop-1` with the sale ID.

```sh
# Audit: runs every detector now and returns the report
curl -s -H "Authorization: Bearer $ADMIN_TOKEN" $API/v1/admin/sales/drop-1/audit

# Kill switch: stop new reservations; orders already in payment continue
curl -s -X POST -H "Authorization: Bearer $ADMIN_TOKEN" -d '{"paused":true}' $API/v1/admin/pause

# Repair Redis drift after re-checking the durable state of each unit
curl -s -X POST -H "Authorization: Bearer $ADMIN_TOKEN" $API/v1/admin/sales/drop-1/repair

# Rebuild Redis inventory from the durable store's FREE units
curl -s -X POST -H "Authorization: Bearer $ADMIN_TOKEN" $API/v1/admin/sales/drop-1/rebuild
```

## First five minutes of any incident

1. Declare the incident in the incident channel and name one incident lead.
2. Check correctness first: run the audit. Any non-zero `oversold_units` or `duplicate_charges` outranks every latency problem.
3. Post a holding status to customers within 10 minutes: "Checkout is slower than usual; your place in the queue is kept."

## Oversell detected

**Alert:** `FlashSaleOversold`. **Severity:** page.

1. Pause reservations with the kill switch.
2. Run the audit and list the affected units from the confirmed orders.
3. Keep the earliest confirmed order for each unit, and refund and notify the others.
4. Find the write path that allowed it before resuming. The guarded claim should make this impossible, so treat it as a code or data bug.

## Duplicate charge detected

**Alert:** `FlashSaleDuplicateCharge`. **Severity:** page.

1. Check whether the extra charge already has a refund pending; the alert excludes those, so this one does not.
2. Refund the extra charge through the provider dashboard, using the attempt ID as the reference.
3. Look for re-sends outside the re-send window in the worker logs.

## Charged without an order

**Alert:** `FlashSaleChargedWithoutOrder`, after 2 minutes. **Severity:** page.

1. A short spike is normal while a webhook or lookup is in flight. Sustained values mean the reconciler is not resolving them.
2. Check the provider status page and the worker's lookup errors.
3. If the order cannot be confirmed, refund it.

## Inventory drift

**Alert:** `FlashSaleInventoryDrift`, after 2 minutes. **Severity:** ticket.

1. Auto-repair fixes drift seen in two consecutive scans. A sustained alert means repair is failing, usually because Redis is unhealthy.
2. Run the repair command manually. If Redis lost data, run the rebuild command.

## Payments stuck past the hard deadline

**Alert:** `FlashSaleStuckPayments`. **Severity:** page.

1. The provider is not answering status lookups. Check its status page.
2. Units were already released; any later success is re-acquired or refunded automatically.
3. Contact the provider with the attempt IDs from the worker logs.

## Latency or error budget burn

**Alerts:** `FlashSaleReservationLatencyHigh`, `FlashSaleErrorBudgetBurn`. **Severity:** page.

1. Check `flashsale_http_inflight_requests` and `flashsale_http_shed_total`. High in-flight with shedding means the fleet is too small.
2. Scale out manually: `kubectl scale deployment/flashsale-api --replicas=<n>`, or raise the KEDA minimum.
3. Slow admissions at the waiting room so origin load matches capacity.
4. If Redis latency is the cause, check for a hot bucket and raise the bucket count for the next sale.

## Redis down

1. Reservations return `503` with `Retry-After`, never a false sold out. Orders already created keep paying.
2. Pause reservations to stop retry pressure while Redis recovers.
3. After recovery, rebuild Redis from the durable store, then unpause.

## Payment provider outage

1. Units with charges in flight stay reserved; nothing is released while outcomes are unknown.
2. Slow admissions to near zero so no new holds pile up.
3. After recovery, the reconciler resolves every UNKNOWN attempt by lookup. Watch `flashsale_stuck_payments`.

## Home region failure

This path is design only. See section 3.8 of the proposal.

1. Confirm the old lease has expired plus the margin, then approve the epoch change.
2. Revoke the old region's provider credential.
3. Wait for the provider request lifetime and ledger delay, run the ledger scan, rebuild Redis, then resume selling.

## Communication

| When | Who | Message |
|---|---|---|
| Within 10 minutes | Customers | Holding status: queue positions are kept |
| Every 30 minutes | Customers and support | Progress and expected recovery |
| After any refund batch | Affected customers | Refund confirmation and apology |
| Within 5 working days | Engineering | Blameless post-incident review |
