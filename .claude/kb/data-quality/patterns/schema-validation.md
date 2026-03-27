# Schema Validation

> **Purpose**: JSON Schema, Pydantic for data validation, Avro/Protobuf enforcement at system boundaries
> **MCP Validated**: 2026-03-26

## When to Use

- Validating API payloads or event schemas before ingestion
- Enforcing data contracts at producer/consumer boundaries
- Schema evolution governance (backward/forward compatibility)
- Python data pipeline input validation

## Implementation

```python
from pydantic import BaseModel, Field, field_validator
from datetime import date, datetime
from enum import Enum
from typing import Optional


class OrderStatus(str, Enum):
    PENDING = "pending"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    REFUNDED = "refunded"


class Order(BaseModel):
    """Order schema — enforced at ingestion boundary."""
    order_id: str = Field(..., pattern=r"^[A-Z0-9]{8,12}$")
    customer_id: str
    amount: float = Field(..., ge=0, le=1_000_000)
    currency: str = Field(..., pattern=r"^[A-Z]{3}$")
    status: OrderStatus
    order_date: date
    created_at: datetime
    metadata: Optional[dict] = None

    @field_validator("customer_id")
    @classmethod
    def validate_customer_id(cls, v: str) -> str:
        if not v.startswith("CUST-"):
            raise ValueError("customer_id must start with 'CUST-'")
        return v


# Validate a batch of records
def validate_batch(records: list[dict]) -> tuple[list[Order], list[dict]]:
    """Returns (valid_records, errors)."""
    valid, errors = [], []
    for i, record in enumerate(records):
        try:
            valid.append(Order(**record))
        except Exception as e:
            errors.append({"row": i, "error": str(e), "data": record})
    return valid, errors


# Usage
valid_orders, validation_errors = validate_batch(raw_records)
if validation_errors:
    print(f"Rejected {len(validation_errors)} of {len(raw_records)} records")
    # Route errors to dead letter queue
```

## Configuration

| Framework | Use Case | Schema Format |
|-----------|----------|--------------|
| Pydantic | Python pipeline validation | Python classes |
| JSON Schema | API contracts, language-agnostic | JSON |
| Avro | Kafka serialization, evolution | `.avsc` JSON |
| Protobuf | gRPC, high-performance serialization | `.proto` |

| Compatibility | Rule | Allowed Changes |
|--------------|------|-----------------|
| Backward | New reader, old writer | Add optional fields, remove fields |
| Forward | Old reader, new writer | Remove optional fields, add fields |
| Full | Both | Add/remove optional fields only |

## Example Usage

```python
# JSON Schema validation (language-agnostic)
import jsonschema

order_schema = {
    "type": "object",
    "required": ["order_id", "amount", "status"],
    "properties": {
        "order_id": {"type": "string"},
        "amount": {"type": "number", "minimum": 0},
        "status": {"type": "string", "enum": ["pending", "completed"]}
    }
}

jsonschema.validate(instance=record, schema=order_schema)
```

## See Also

- [data-contracts](../concepts/data-contracts.md)
- [data-contract-authoring](../patterns/data-contract-authoring.md)
