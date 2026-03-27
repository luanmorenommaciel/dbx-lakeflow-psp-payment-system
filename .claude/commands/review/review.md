---
name: review
description: Dual AI code review with CodeRabbit + Claude Code for maximum coverage
---

# Review Command

> Dual AI code review with CodeRabbit + Claude Code for maximum coverage

## Usage

```bash
/review                        # Review all changes vs main
/review uncommitted            # Review only uncommitted changes
/review committed              # Review only committed changes
/review --base develop         # Compare to specific branch
/review --quick                # CodeRabbit only (faster)
/review --deep                 # Claude only (no CodeRabbit)
```

---

## Overview

This command orchestrates a **dual AI review** combining:

| Reviewer | Strengths |
|----------|-----------|
| **CodeRabbit** | Static analysis, security scanning (Gitleaks, Semgrep), linting (Ruff, Pylint), pattern detection |
| **Claude** | Architectural review, business logic, design patterns, contextual understanding |

```text
┌─────────────────────────────────────────────────────────────────┐
│                    DUAL AI REVIEW PIPELINE                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   ┌───────────────┐          ┌───────────────┐                  │
│   │  CodeRabbit   │          │    Claude     │                  │
│   │     CLI       │          │    Code       │                  │
│   └───────┬───────┘          └───────┬───────┘                  │
│           │                          │                           │
│   ┌───────▼───────┐          ┌───────▼───────┐                  │
│   │ • Security    │          │ • Architecture│                  │
│   │ • Linting     │          │ • Logic       │                  │
│   │ • Patterns    │          │ • Design      │                  │
│   │ • Style       │          │ • Intent      │                  │
│   └───────┬───────┘          └───────┬───────┘                  │
│           │                          │                           │
│           └────────────┬─────────────┘                          │
│                        │                                         │
│                ┌───────▼───────┐                                 │
│                │   UNIFIED     │                                 │
│                │   REPORT      │                                 │
│                └───────────────┘                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Process

### Step 1: Determine Scope

```bash
# Check current state
git status
git diff --stat HEAD
git log origin/main..HEAD --oneline 2>/dev/null || echo "No commits ahead of main"
```

**Scope Selection:**

| User Input | Scope |
|------------|-------|
| `/review` | All changes vs main |
| `/review uncommitted` | Working directory only |
| `/review committed` | Committed changes only |
| `/review --base <branch>` | Compare to specific branch |

### Step 2: Run CodeRabbit Analysis

```bash
# Source shell to ensure CLI is available
source ~/.zshrc

# Check CLI availability
coderabbit --version || echo "CodeRabbit CLI not available"

# Run review based on mode
# For all changes:
coderabbit review --plain

# For uncommitted only:
coderabbit review --type uncommitted --plain

# For committed only:
coderabbit review --type committed --plain

# For specific base branch:
coderabbit review --base <branch> --plain
```

**Note:** Requires CodeRabbit GitHub App installed on the repository.

**Parse CodeRabbit Output:**

```text
SEVERITY MAPPING
├─ [CRITICAL] → Must fix before merge
├─ [ERROR]    → Should fix before merge
├─ [WARNING]  → Recommended to fix
└─ [INFO]     → Nice to have
```

### Step 3: Run Claude Deep Analysis

Use the code-reviewer agent capabilities:

**Focus Areas:**

| Category | Check For |
|----------|-----------|
| **Architecture** | Project pattern alignment, separation of concerns |
| **Business Logic** | Correct implementation, edge cases, error handling |
| **Design Patterns** | Project conventions, established patterns, consistency |
| **Maintainability** | Self-documenting code, type hints, DRY principle |

### Step 4: Synthesize Findings

Combine results from both reviewers:

1. **Deduplicate** — Same issue found by both → keep one, note "Both"
2. **Prioritize** — Critical > Error > Warning > Info
3. **Categorize** — Security, Quality, Performance, Style
4. **Action** — Must fix vs Should fix vs Nice to have

### Step 5: Generate Report

---

## Output Format

```markdown
## 🔍 Dual AI Review Report

**Reviewers:** CodeRabbit + Claude Code
**Scope:** {scope_description}
**Files:** {count} files, {lines} lines changed
**Date:** {timestamp}

---

### 📊 Summary

| Source | 🔴 Critical | 🟠 Error | 🟡 Warning | 🔵 Info |
|--------|-------------|----------|------------|---------|
| CodeRabbit | {n} | {n} | {n} | {n} |
| Claude | {n} | {n} | {n} | {n} |
| **Total** | {n} | {n} | {n} | {n} |

---

### 🔴 Critical Issues

> Must fix before merge

#### [C1] {Title}
- **Source:** {CodeRabbit|Claude|Both}
- **File:** `{path}:{line}`
- **Issue:** {description}
- **Fix:**
```{lang}
{code}
```

---

### 🟠 Errors

> Should fix before merge

#### [E1] {Title}
- **Source:** {source}
- **File:** `{path}:{line}`
- **Issue:** {description}

---

### 🟡 Warnings

> Recommended to fix

- [{source}] `{file}`: {description}

---

### 🔵 Suggestions

- {suggestion 1}
- {suggestion 2}

---

### ✅ Positive Observations

- {good practice 1}
- {good practice 2}

---

### 📋 Action Checklist

- [ ] Fix: {critical 1}
- [ ] Fix: {critical 2}
- [ ] Consider: {warning 1}

---

**Merge Status:** {✅ Ready | ⚠️ Fix warnings first | 🚫 Fix critical issues}
```

---

## Error Handling

### CodeRabbit CLI Not Available

```text
IF coderabbit command not found:
  1. Attempt: source ~/.zshrc && coderabbit --version
  2. If still fails: Proceed with Claude-only review
  3. Note in report: "CodeRabbit unavailable"
```

### CodeRabbit Not Authenticated

```text
IF authentication error:
  1. Inform user: "Run 'coderabbit auth login' to authenticate"
  2. Proceed with Claude-only review
  3. Note in report: "CodeRabbit not authenticated"
```

### Large Changeset

```text
IF > 50 files changed:
  1. Suggest: "Large changeset detected. Use '/review uncommitted' for faster feedback"
  2. Proceed with review but note potential timeout
```

---

## Quick Mode (`--quick`)

CodeRabbit only — for fast feedback:

```bash
/review --quick
```

**Process:**
1. Run `coderabbit review --plain`
2. Parse and format results
3. Skip Claude analysis
4. Return immediately

**Use When:**
- Quick sanity check
- Pre-commit validation
- CI/CD integration

---

## Deep Mode (`--deep`)

Claude only — for thorough analysis:

```bash
/review --deep
```

**Process:**
1. Skip CodeRabbit
2. Full Claude analysis with all capabilities
3. Detailed architectural review
4. Extended recommendations

**Use When:**
- CodeRabbit unavailable
- Need deeper contextual analysis
- Reviewing design decisions

---

## Integration

### Before PR Creation

```bash
# Review first, then create PR
/review
# If all good:
/create-pr
```

### With create-pr Command

```bash
# Automatically runs review before PR
/create-pr --review
```

### In Development Loop

```bash
# Quick feedback on work in progress
/review uncommitted

# Full review before commit
git add .
/review committed
```

---

## Configuration

Optionally respects `.coderabbit.yaml` settings (create this file if needed):

| Setting | Effect |
|---------|--------|
| `reviews.path_instructions` | Custom rules per path |
| `reviews.tools` | Which linters are enabled |
| `reviews.pre_merge_checks` | Quality gates |

---

## Examples

### Example 1: Standard Review

```bash
/review

# Output:
## 🔍 Dual AI Review Report
...
🔴 Critical: 0
🟠 Error: 2
🟡 Warning: 5

Merge Status: ⚠️ Fix errors first
```

### Example 2: Quick Check

```bash
/review --quick uncommitted

# Output:
## CodeRabbit Quick Review
...
✅ No critical issues
⚠️ 3 warnings to consider
```

### Example 3: Branch Comparison

```bash
/review --base develop

# Output:
## 🔍 Dual AI Review Report
Comparing: HEAD vs develop
...
```

---

## Tips

1. **Review Early** — Run `/review uncommitted` frequently during development
2. **Fix Critical First** — Always address critical and error issues before PR
3. **Learn from Feedback** — Both AIs provide educational explanations
4. **Use Quick Mode** — For rapid iteration, `/review --quick` is your friend
5. **Pre-PR Habit** — Always `/review` before `/create-pr`

---

## Related

- Agent: `.claude/agents/python/code-reviewer.md`
- Config: `.coderabbit.yaml` (optional, create if needed)
- Create PR: `.claude/commands/workflow/create-pr.md`
