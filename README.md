# Asynchronous Airtable Python Wrapper
[![Python 3.7](https://img.shields.io/badge/python-3.7-blue.svg)](https://www.python.org/downloads/release/python-370)
[![Python 3.8](https://img.shields.io/badge/python-3.8-blue.svg)](https://www.python.org/downloads/release/python-380)

[![PyPI version](https://badge.fury.io/py/airtable-async.svg)](https://badge.fury.io/py/airtable-async)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/airtable-async.svg?label=pypi%20downloads)](https://pypi.org/project/airtable-async/)
[![Build Status](https://travis-ci.org/lfparis/airbase.svg?branch=master)](https://travis-ci.org/lfparis/airbase)
[![Coverage Status](https://coveralls.io/repos/github/lfparis/airbase/badge.svg?branch=master)](https://coveralls.io/github/lfparis/airbase?branch=master)

## Installing
```bash
pip install airtable-async
```
Requirements: Python 3.7+

## Documentation
*coming soon*

## Example

```python
import asyncio
from airbase import Airtable

api_key = "your Airtable API key found at https://airtable.com/account"
base_key = "name or id of a base"
table_key = "name or id of a table in that base"


async def main() -> None:
    async with Airtable(api_key=api_key) as at:
        at: Airtable

        # Get all bases for a user
        await at.get_bases()

        # Get one base by name
        base = await at.get_base(base_key, key="name")
        # Get one base by id
        base = await at.get_base(base_key, key="id")
        # Get one base by either id or name
        base = await at.get_base(base_key)

        # Base Attributes
        print(base.id)
        print(base.name)
        print(base.permission_level)

        # Set base logging level (debug, info, warning, error, etc)
        # Default is "info"
        base.log = "debug"

        # Get all tables for a base
        await base.get_tables()

        # Get one table by name
        table = await base.get_table(table_key, key="name")
        # Get one table by id
        table = await base.get_table(table_key, key="id")
        # Get one table by either id or name
        table = await base.get_table(table_key)

        # Base Attributes
        print(table.base)
        print(table.name)
        print(table.id)
        print(table.primary_field_id)
        print(table.primary_field_name)
        print(table.fields)
        print(table.views)

        # Get a record in that table
        table_record = await table.get_record("record_id")
        # Get all records in that table
        table_records = await table.get_records()
        # Get all records in a view in that table
        view_records = await table.get_records(view="view id or name")
        # Get only certain fields for all records in that table
        reduced_fields_records = await table.get_records(
            filter_by_fields=["field1, field2"]
        )
        # Get all records in that table that pass a formula
        filtered_records = await table.get_records(
            filter_by_formula="Airtable Formula"
        )

        # Post a record in that table
        record = {"fields": {"field1": "value1", "field2": "value2"}}
        await table.post_record(record)
        # Post several records in that table
        records = [
            {"fields": {"field1": "value1", "field2": "value2"}},
            {"fields": {"field1": "value1", "field2": "value2"}},
            {"fields": {"field1": "value1", "field2": "value2"}},
        ]
        await table.post_records(records)

        # Update a record in that table
        record = {
            "id": "record id",
            "fields": {"field1": "value1", "field2": "value2"},
        }
        await table.update_record(record)
        # Update several records in that table
        records = [
            {
                "id": "record id",
                "fields": {"field1": "value1", "field2": "value2"},
            },
            {
                "id": "record id",
                "fields": {"field1": "value1", "field2": "value2"},
            },
            {
                "id": "record id",
                "fields": {"field1": "value1", "field2": "value2"},
            },
        ]
        await table.update_records(records)

        # Delete a record in that table
        record = {
            "id": "record id",
        }
        await table.delete_record(record)
        # Delete several records in that table
        records = [
            {"id": "record id"},
            {"id": "record id"},
            {"id": "record id"},
        ]
        await table.delete_records(records)


if __name__ == "__main__":
    asyncio.run(main())
```

## License

[MIT](https://opensource.org/licenses/MIT)