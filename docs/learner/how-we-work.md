# How we work

Effective teams close a named ticket with proof. This workshop uses the same loop. Walk it clockwise. After
step 6 you are back at 1 with a smaller remaining problem.

```text
1 Own the outcome → 2 Write the plan → 3 Execute the slice
        ↑                                      ↓
6 Teach the house   ←  5 Human review  ←  4 Verify in the loop
                           Evidence sits in the center
```

| Step | In this room | Proof |
|---|---|---|
| **1 Own** | You own `m-007` becoming explainable. Read the [BRD](brd-psp.md). | You can state the 25 → 45 delta before you paste a prompt. |
| **2 Plan** | Paste the module prompt. The agent must propose a plan and wait for your approval. | A scoped plan. No files until you say yes. |
| **3 Execute** | The agent implements only the paths in that prompt. Divide the problem; one module at a time. | Diff matches the prompt. Advanced reference untouched. |
| **4 Verify** | `./scripts/checkpoint.sh NN` | `CHECKPOINT=PASS module=NN` |
| **5 Review** | You inspect evidence. You sign deploy in module 05. | Gold, quarantine, or CLI receipts you can point at. |
| **6 Lesson** | Copy [lesson-template.md](lesson-template.md) to `.workshop-evidence/lessons/mNN.md`. | One thing to keep, one thing not to repeat. |

Evidence is the hub: tests, checkpoint output, manifests, Gold scores, Genie SQL. Chat is not the hub.

Orange gates: a named owner (you), and proof you can point at. Do not call a fallback a live success.
