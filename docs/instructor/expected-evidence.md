# Expected evidence

| Stage | Expected evidence |
|---|---|
| Opening | learners can state the `m-007` 25 → 45 ticket from `docs/learner/brd-psp.md` |
| Contract | four entities, seed 22082026, eight ledger entries |
| Lesson | `.workshop-evidence/lessons/mNN.md` exists after each checkpoint |
| Skills | selected Databricks skills listed for Claude Code and Codex |
| Generator | 100,000 transactions; 98,791 baseline and 1,209 incident |
| Bronze | four graph nodes with ingestion metadata |
| Drift | hosted Auto Loader retains the drift row in `_rescued_data` |
| DQX | six transaction reasons, one temporal dispute reason, `_errors`/`_warnings` |
| Gold | `m-007` has 1,209 quarantined transactions and risk 25 before replay, then risk 45 |
| DAB | strict validation, deployment summary, CLI upload count, and pipeline update ID |
| Genie Code | four prompts tied to expected result shapes and inspected SQL |
| Replay | late chargeback passes Silver and increases merchant risk |

## Release receipts

- Local: `.workshop-evidence/local/04-baseline-assert.log` and `06-replay-assert.log` must contain explicit
  `LOCAL_ASSERT=PASS` lines.
- Hosted: `.workshop-evidence/remote/` must contain preflight, dry-run, strict validation, deployment, run,
  baseline SQL, Genie, replay, and post-replay SQL evidence from one selected profile.
- Final markers: `LOCAL_E2E=PASS` and `REMOTE_E2E=PASS` are required before calling the complete workshop live-ready.
- Release: checkpoint-tag, Claude Code, Codex, fallback, and website receipts must each state `state=PASS`.
- Credentials, profile contents, raw debug traces, and personally identifying workspace output must never be
  copied into `docs/`.
