---
title: Watch Ring
summary: In-memory watch event ring and subscription behavior.
audience: contributors
status: canonical
last_verified: 2026-03-03
source_of_truth:
  - crates/astra-core/src/watch.rs
related_artifacts:
  - refs/scripts/validation/phase5-scenario-watch-crucible.sh
---

# Watch Ring

Astra publishes watch events through an in-memory ring plus broadcast channel.

## Key Properties

- avoids disk scan replay for active watch fanout,
- bounded ring capacity,
- backlog mode controls (`strict` vs `relaxed`).

## Runtime Considerations

- stream cardinality matters for high fanout tests,
- monitor dropped-no-subscriber behavior in relaxed mode.
