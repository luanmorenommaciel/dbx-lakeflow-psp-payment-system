# Workshop fallbacks

- Data: run `scripts/generate_fallback.sh`; use `data/fallback/generated/` and verify its manifest.
- DQX: pin `databricks-labs-dqx==0.16.0`; run `./scripts/cache_dqx_wheel.sh` before class to cache the exact
  checksum-verified PyPI wheel under ignored evidence storage.
- Replay: `./scripts/release_incident.sh --local` preserves the prior local warehouse under `/tmp` and performs
  a full replay. Label it local fallback evidence, not a hosted incremental update.
- Local quality: the open-source Spark runner uses the same result schema and rule outcomes without invoking
  DQX's eager planner path. Only the hosted pipeline can provide DQX runtime proof.
- Pipeline: keep sanitized dry-run output and the graph image in `docs/fallback/dry-run/` after rehearsal.
- Genie: keep the exact prompts, generated SQL, and bounded results in `docs/fallback/genie/` after rehearsal.

Current checked-in snapshots are deliberately labeled local: the SDP dry-run proves graph construction, and the
query replay records deterministic expected values without claiming Genie runtime. Replace or supplement them
only with sanitized receipts from an authorized hosted rehearsal. Do not manufacture live artifacts.
