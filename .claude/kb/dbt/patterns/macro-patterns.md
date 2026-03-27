# Macro Patterns

> **Purpose**: Jinja control flow, cross-database macros, grants, and whitespace control
> **MCP Validated**: 2026-03-26

## When to Use

- Generating repetitive SQL across many models (DRY principle)
- Supporting multiple warehouse platforms with a single codebase
- Encapsulating business logic that changes independently of model SQL
- Post-run operations like granting permissions or logging

## Implementation

```sql
-- macros/generate_schema_name.sql
-- Override default schema generation for environment isolation

{% macro generate_schema_name(custom_schema_name, node) %}
    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif target.name == 'prod' -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{% endmacro %}
```

```sql
-- macros/cents_to_dollars.sql
-- Reusable currency conversion with null safety

{% macro cents_to_dollars(column_name, precision=2) %}
    round(cast({{ column_name }} as decimal(18, {{ precision }})) / 100, {{ precision }})
{% endmacro %}
```

```sql
-- macros/grant_select.sql
-- Post-hook macro to grant read access

{% macro grant_select(role) %}
    {%- set sql -%}
        grant select on {{ this }} to role {{ role }}
    {%- endset -%}
    {% if execute %}
        {% do run_query(sql) %}
        {{ log("Granted select on " ~ this ~ " to " ~ role, info=true) }}
    {% endif %}
{% endmacro %}
```

```sql
-- macros/union_sources.sql
-- Dynamically union multiple source tables with the same schema

{% macro union_sources(schema_name, table_prefix, source_list) %}

    {%- for source_name in source_list %}
        select
            '{{ source_name }}' as source_system,
            *
        from {{ source(schema_name, table_prefix ~ '_' ~ source_name) }}
        {% if not loop.last %}union all{% endif %}
    {%- endfor %}

{% endmacro %}
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `macro-paths` | `["macros"]` | Directories to scan for macros |
| `dispatch` | `[]` | Package search order for adapter.dispatch() |
| `post-hook` | `[]` | SQL/macros to run after model builds |
| `pre-hook` | `[]` | SQL/macros to run before model builds |

## Example Usage

```yaml
# dbt_project.yml — apply grant macro as post-hook
models:
  analytics:
    marts:
      +post-hook:
        - "{{ grant_select('analyst_role') }}"
```

```sql
-- Using cents_to_dollars in a model
select
    order_id,
    {{ cents_to_dollars('amount_cents') }} as amount_dollars
from {{ ref('stg_payments') }}
```

## See Also

- [generic-tests](../patterns/generic-tests.md)
- [model-types](../concepts/model-types.md)
