# Foundations

These ideas are the product. The tools in [techs.md](techs.md) only implement them.

## Medallion

Payment events land raw, become trustworthy, then become a decision.

- **Bronze** keeps every row and the file it came from. Schema may evolve. Rescued fields stay queryable.
- **Silver** is the domain table: valid merchants, orders, transactions, and disputes.
- **Quarantine** is still Silver's sibling, not a trash can. Invalid rows stay with `_errors` and `_warnings`.
- **Gold** is one merchant-grain view, `gold_merchant_risk`. It is the number an operations analyst can act on.

If Bronze drops a row, lineage is a lie. If Silver drops a row, Gold cannot explain `m-007`.

## Keep-don't-drop quality

DQX is the only place domain rules live. The workshop forbids `ON VIOLATION DROP ROW`. Unauthorized BRL,
null IDs, and broken dispute timelines must remain visible. The analyst's job is to see *why* a payment was
rejected, not to pretend it never arrived.

## Shared namespace

Unity Catalog is the room's shared language: one learner schema, one managed Volume, inspectable tables and
lineage. Genie Code reads those tables. If the name is not in Unity Catalog, the investigation is a screenshot.

## Human gates

The learner owns the ticket. The agent proposes. You approve the plan before files change, and you approve
deploy before any workspace mutation. Local tests, pipeline dry-run, bundle validation, deployment, and
runtime queries prove different things. Do not substitute one for another.

## Evidence is observed

Done is a checkpoint line, a Gold score, a quarantine reason, or a Genie SQL result. Chat tokens are not
evidence. Label every fact as local, remote dry-run, deployed, live runtime, or fallback.
