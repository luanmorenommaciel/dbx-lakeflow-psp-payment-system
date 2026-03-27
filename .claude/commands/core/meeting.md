---
name: meeting
description: Analyze meeting transcripts — extract decisions, action items, and create SSOT documents
---

# /meeting Command

> Analyze meeting transcripts and extract structured, actionable documentation.

## Usage

```bash
# Analyze a meeting transcript
/meeting notes/standup-2026-03-26.md

# Analyze and create requirements
/meeting notes/stakeholder-review.md --output define

# Consolidate multiple meetings
/meeting notes/sprint-*.md --consolidate
```

## What It Does

1. Invokes the **meeting-analyst** agent
2. Reads meeting transcript (notes, recording summary, or raw text)
3. Extracts:
   - Decisions made (with who decided and context)
   - Action items (with owner, deadline, priority)
   - Open questions (unresolved, needs follow-up)
   - Key insights (non-obvious observations)
   - Data engineering context (if present: sources, SLAs, schema changes)

## Output Format

```markdown
## Meeting Analysis: {Title}

### Decisions
| # | Decision | Decided By | Context |
|---|----------|-----------|---------|

### Action Items
| # | Action | Owner | Deadline | Priority |
|---|--------|-------|----------|----------|

### Open Questions
| # | Question | Blocker? | Follow-up |
|---|----------|----------|-----------|

### Key Insights
- {Non-obvious observation}
```

## Integration with SDD

Use `--output define` to generate a DEFINE document from meeting notes — the meeting-analyst extracts requirements and feeds them into the define-agent.
