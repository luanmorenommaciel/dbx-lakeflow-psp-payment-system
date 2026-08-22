# Module 06 — Genie Code investigation and delivery

## How we work this slice

1. Own: four Genie questions plus the valid replay that moves `m-007` from 25 to 45.
2. Plan: propose four bounded investigation questions and a delivery plan, then wait for my approval.
3. Execute: create only the learner-owned assets under `docs/genie/`.
4. Verify: `./scripts/checkpoint.sh 06`
5. Review: inspect generated SQL; quality evidence is not a fraud claim.
6. Lesson: copy `docs/learner/lesson-template.md` to `.workshop-evidence/lessons/m06.md`

Read `AGENTS.md`, the product specification, and the Genie skill. First propose four bounded investigation
questions and a delivery plan, then wait for my approval.

After approval, create the learner-owned assets under `docs/genie/`: four questions, expected factual results,
and investigation instructions. Genie Code is the required interface. It must show generated SQL and distinguish
observed data-quality signals from claims of fraud. A reusable Genie Space may be retained as optional instructor
material, but it cannot block this module.

Run `./scripts/checkpoint.sh 06`. Then guide me through the four questions in Genie Code, the valid late-chargeback
release, hosted replay verification, and the `m-007` risk change from 25 to 45. Do not perform a remote mutation
without my explicit approval. Finish by labelling each item as local, deployed, live runtime, or fallback evidence.
