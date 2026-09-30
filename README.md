# FabWeave Operations Lab

FabWeave is a dependency-light Python reference project for semiconductor-fab
operations.  Its production modules model queue-time protection, dispatch,
reticle reservations, recipe qualification, SPC, metrology sampling,
maintenance, chamber matching, holds, genealogy, chemical aging, cleanroom
excursions, demand control, rework, lot merging, equipment dedication,
contamination, takt, route yield, shipment commitments and the cross-plant
transfer scan chain.

The transfer scan chain links load, departure, arrival and unload scans into
one trustworthy stream: stages must advance in order, duplicate scans never
book twice, skipped or reversed scans return explicit rejection reasons,
active holds block departure, and a completed transfer reconnects the
destination carriers to the original material genealogy.

The cross-domain workflow layer composes these services for material
readiness, dispatch-after-hold, qualified reservations and assignments,
metrology/SPC gates, containment, powered scheduling, rework release,
merge-to-commit, hold-gated transfer scans with lineage reattachment and
performance reporting.  It preserves domain rejection reasons instead of
replacing them with a generic success or failure.

Shared infrastructure supplies half-open time intervals, versioned policy
history, immutable decisions, quantity conservation and an append-only event
log with optimistic concurrency.  Domain services use these primitives to keep
event-time, idempotency and fail-closed behavior consistent.

Run the tests with `python -m pytest -q`.
