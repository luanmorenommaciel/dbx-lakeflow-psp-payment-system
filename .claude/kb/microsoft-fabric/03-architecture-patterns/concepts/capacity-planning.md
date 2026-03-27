> **MCP Validated:** 2026-02-17

# Capacity Planning

> **Purpose**: Fabric capacity units, F-SKUs, throttling, autoscale, and sizing guidance
> **Confidence**: 0.95

## Overview

Microsoft Fabric billing revolves around **Capacity Units (CUs)**, a unified compute currency consumed by all workloads (Lakehouse, Warehouse, Pipelines, Notebooks, Power BI). Capacity is provisioned via **F-SKUs** ranging from F2 (dev/test) to F2048 (enterprise). Understanding CU consumption, throttling behavior, and autoscale is essential for cost control and performance.

## F-SKU Reference

| SKU | CUs | Max Memory | Concurrent Requests | Use Case |
|-----|-----|------------|---------------------|----------|
| F2 | 2 | 3 GB | Low | Dev/test, POC |
| F4 | 4 | 6 GB | Low | Small team dev |
| F8 | 8 | 12 GB | Medium | Small production |
| F16 | 16 | 24 GB | Medium | Departmental |
| F32 | 32 | 48 GB | Medium-High | Mid-size production |
| F64 | 64 | 96 GB | High | Large production |
| F128 | 128 | 192 GB | High | Enterprise |
| F256 | 256 | 384 GB | Very High | Large enterprise |
| F512 | 512 | 768 GB | Very High | Mission-critical |
| F1024 | 1024 | 1536 GB | Maximum | Heavy analytics |
| F2048 | 2048 | 3072 GB | Maximum | Largest workloads |

## Key Concepts

### CU Consumption Model

```text
CAPACITY POOL (e.g., F64 = 64 CUs)
  |
  +-- Warehouse queries     --> CU seconds consumed per query
  +-- Spark notebooks       --> CU seconds based on cluster size
  +-- Pipeline activities   --> CU seconds per activity run
  +-- Dataflow Gen2         --> CU seconds per refresh
  +-- Power BI (Direct Lake)--> CU seconds per visual query
  |
  +-- Smoothing: Usage averaged over 24-hour windows
  +-- Bursting: Short spikes above capacity are allowed
  +-- Throttling: Sustained overuse triggers request delays
```

### Throttling Stages

| Stage | Trigger | Impact |
|-------|---------|--------|
| **Smoothing** | Usage spike < 10 min | No impact; averaged over window |
| **Throttling** | Sustained > 100% CU | Interactive requests delayed |
| **Rejection** | Sustained > 120% CU | Background jobs rejected |
| **Suspension** | Extreme overuse | All requests rejected until cooldown |

### Autoscale

- Autoscale adds up to **one additional capacity** on top of base SKU
- Triggered when sustained CU usage exceeds capacity threshold
- Billed per-second for additional CUs consumed
- Enable in Fabric Admin Portal under capacity settings
- Cap autoscale maximum CU to control cost

## Sizing Guidance

| Workload Profile | Recommended SKU | Rationale |
|-----------------|-----------------|-----------|
| POC / Dev (1-3 users) | F2 or F4 | Minimal cost, sufficient for testing |
| Small team (5-10 users) | F8 or F16 | Light concurrent queries |
| Department (10-50 users) | F32 or F64 | Moderate concurrency + pipelines |
| Enterprise (50-200 users) | F128 or F256 | Heavy BI + data engineering |
| Large enterprise (200+ users) | F512+ | Peak concurrency + autoscale |

## Capacity Reservations

- **Pay-as-you-go**: Per-second billing, can pause capacity
- **Reserved (1-year)**: ~20% discount over PAYG
- **Reserved (3-year)**: ~40% discount over PAYG
- Reservations apply to a specific Azure region
- Pause capacity during off-hours to save costs (PAYG only)

## Common Mistakes

### Wrong

```text
Provisioning F512 for a 10-person team "just in case"
--> Over-provisioned capacity wastes budget with no benefit
```

### Correct

```text
Start with F16, monitor CU utilization via Capacity Metrics app,
enable autoscale with a cap, and right-size after 2-4 weeks of data
```

## Monitoring

```text
Fabric Admin Portal --> Capacity Metrics App
  |
  +-- CU utilization % (target: 60-80% sustained)
  +-- Throttling events (target: 0 per day)
  +-- Per-workload breakdown (identify top consumers)
  +-- Timepoint analysis (identify peak hours)
```

## Related

- [Workload Selection](workload-selection.md)
- [Workspace Design](workspace-design.md)
- [Hybrid Architecture](../patterns/hybrid-architecture.md)
