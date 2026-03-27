# Pub/Sub Terraform Module

> **Purpose**: Reusable module for creating Pub/Sub topics, subscriptions, and dead-letter queues
> **MCP Validated**: 2026-02-17

## When to Use

- Creating event-driven messaging infrastructure
- Standardizing Pub/Sub topic and subscription patterns
- Implementing dead-letter queues for failed message handling

## Implementation

```hcl
# modules/pubsub/variables.tf
variable "project_id" {
  type        = string
  description = "GCP project ID"
}

variable "topic_name" {
  type        = string
  description = "Name of the Pub/Sub topic"
}

variable "message_retention_duration" {
  type        = string
  default     = "86400s"
  description = "How long to retain unacknowledged messages (default 24h)"
}

variable "subscriptions" {
  type = map(object({
    ack_deadline_seconds = optional(number, 60)
    push_endpoint        = optional(string, null)
    filter               = optional(string, null)
    retry_minimum_backoff = optional(string, "10s")
    retry_maximum_backoff = optional(string, "600s")
  }))
  default     = {}
  description = "Map of subscription configurations"
}

variable "enable_dead_letter" {
  type    = bool
  default = false
}

variable "max_delivery_attempts" {
  type    = number
  default = 5
}

variable "labels" {
  type    = map(string)
  default = {}
}
```

```hcl
# modules/pubsub/main.tf
resource "google_pubsub_topic" "topic" {
  name                       = var.topic_name
  project                    = var.project_id
  message_retention_duration = var.message_retention_duration
  labels                     = var.labels
}

# Dead letter topic (optional)
resource "google_pubsub_topic" "dead_letter" {
  count   = var.enable_dead_letter ? 1 : 0
  name    = "${var.topic_name}-dead-letter"
  project = var.project_id
  labels  = var.labels
}

# Subscriptions
resource "google_pubsub_subscription" "subscriptions" {
  for_each = var.subscriptions

  name    = "${var.topic_name}-${each.key}"
  project = var.project_id
  topic   = google_pubsub_topic.topic.id

  ack_deadline_seconds = each.value.ack_deadline_seconds
  filter               = each.value.filter

  retry_policy {
    minimum_backoff = each.value.retry_minimum_backoff
    maximum_backoff = each.value.retry_maximum_backoff
  }

  dynamic "push_config" {
    for_each = each.value.push_endpoint != null ? [1] : []
    content {
      push_endpoint = each.value.push_endpoint
    }
  }

  dynamic "dead_letter_policy" {
    for_each = var.enable_dead_letter ? [1] : []
    content {
      dead_letter_topic     = google_pubsub_topic.dead_letter[0].id
      max_delivery_attempts = var.max_delivery_attempts
    }
  }

  labels = var.labels
}
```

```hcl
# modules/pubsub/outputs.tf
output "topic_id" {
  value = google_pubsub_topic.topic.id
}

output "topic_name" {
  value = google_pubsub_topic.topic.name
}

output "subscription_ids" {
  value = { for k, v in google_pubsub_subscription.subscriptions : k => v.id }
}

output "dead_letter_topic_id" {
  value = var.enable_dead_letter ? google_pubsub_topic.dead_letter[0].id : null
}
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `message_retention_duration` | `86400s` | Unacked message retention (24h) |
| `ack_deadline_seconds` | `60` | Time to acknowledge before redelivery |
| `max_delivery_attempts` | `5` | Attempts before dead-letter |
| `retry_minimum_backoff` | `10s` | Minimum retry wait |
| `retry_maximum_backoff` | `600s` | Maximum retry wait |

## Example Usage

```hcl
module "invoice_events" {
  source = "./modules/pubsub"

  project_id = var.project_id
  topic_name = "invoice-events"

  enable_dead_letter    = true
  max_delivery_attempts = 5

  subscriptions = {
    processor = {
      ack_deadline_seconds = 120
    }
    analytics = {
      push_endpoint = module.analytics_api.service_url
    }
    audit = {
      filter = "attributes.type = \"invoice.created\""
    }
  }

  labels = {
    domain      = "invoicing"
    environment = var.environment
  }
}
```

## See Also

- [Cloud Run Module](../patterns/cloud-run-module.md)
- [GCS Module](../patterns/gcs-module.md)
- [Resources](../concepts/resources.md)
