# Progressive curriculum release

The curriculum is a linear `workshop-v1` branch. `main` remains the instructor solution. Every release tag is
annotated and immutable; never move a published checkpoint tag.

| Tag | Student product present |
|---|---|
| `workshop-v1-starter` | governance, specification, prompts, tests, fallback, recovery only |
| `workshop-v1-m01-contract-plan` | payment contract and incident ledger |
| `workshop-v1-m02-synthetic-data` | deterministic generator and replay builder |
| `workshop-v1-m03-bronze` | Bronze-only cumulative SDP graph |
| `workshop-v1-m04-dqx-silver` | DQX, Silver, quarantine cumulative graph |
| `workshop-v1-m05-gold-dabs` | Gold, dev bundle, full graph |
| `workshop-v1-m06-genie-delivered` | four Genie Code investigation assets |
| `workshop-v1-solution` | same completed product as the instructor solution |

Run `./scripts/verify_checkpoints.sh` from the instructor solution before publication. It proves the starter has
no completed product and runs each cumulative checkpoint in a detached temporary worktree. The script records
`.workshop-evidence/rehearsals/checkpoints.env`; publication is a separate human-controlled Git push.

Fresh-start Claude Code and Codex rehearsals must use separate branches cloned from `workshop-v1-starter`. Record
their elapsed time, failed/recovered module, final commit, and `state=PASS` in their named rehearsal receipts only
after all six outcomes are independently reached.
