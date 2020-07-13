# Airtable Python Wrapper

### Asynchronous
Requires CPython 3.8
```python
import asyncio
from airbase import AirtableAsync as Airtable

api_key = "your Airtable API key found at https://airtable.com/account"
base_key = "name or id of a base"
table_key = "name or id of a table in that base"


async def main() -> None:
    async with Airtable(api_key=api_key) as at:

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
        table = await at.get_table(table_key, key="name")
        # Get one table by id
        table = await at.get_table(table_key, key="id")
        # Get one table by either id or name
        table = await at.get_table(table_key)

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
        await table.update_record()
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
        await table.update_records()

        # Delete a record in that table
        record = {
            "id": "record id",
        }
        await table.delete_record()
        # Delete several records in that table
        records = [
            {"id": "record id"},
            {"id": "record id"},
            {"id": "record id"},
        ]
        await table.delete_records()


if __name__ == "__main__":
    asyncio.run(main())
```

### Synchronous
Works in ironpython and cpython 2.7 and beyond
```python
from airbase import Airtable

api_key = "your Airtable API key found at https://airtable.com/account"
base_key = "id of a base"
table_key = "name of a table in that base"


def main() -> None:
    # NOT IMPLEMENTED - with Airtable(api_key=api_key) as at: 
    at = Airtable(api_key=api_key)

    # Get all bases for a user
    at.get_bases()

    # NOT IMPLEMENTED - Get one base by name
    # base = at.get_base(base_key, key="name")
    # Get one base by id
    base = at.get_base(base_key)
    # NOT IMPLEMENTED - Get one base by either id or name
    # base = at.get_base(base_key)

    # Base Attributes
    print(base.id)
    print(base.name)
    print(base.permission_level)

    # Set base logging level (debug, info, warning, error, etc)
    # Default is "info"
    base.log = "debug"

    # Get all tables for a base
    base.get_tables()

    # Get one table by name
    table = at.get_table(table_key)
    # NOT IMPLEMENTED - Get one table by id
    # table = at.get_table(table_key, key="id")
    # NOT IMPLEMENTED - Get one table by either id or name
    # table = at.get_table(table_key)

    # Base Attributes
    print(table.base)
    print(table.name)
    print(table.id)
    print(table.primary_field_id)
    # print(table.primary_field_name)
    print(table.fields)
    print(table.views)

    # Get a record in that table
    table_record = table.get_record("record_id")
    # Get all records in that table
    table_records = table.get_records()
    # NOT IMPLEMENTED - Get all records in a view in that table
    # view_records = table.get_records(view="view id or name")
    # Get only certain fields for all records in that table
    reduced_fields_records = table.get_records(
        filter_by_fields=["field1, field2"]
    )
    # Get all records in that table that pass a formula
    filtered_records = table.get_records(
        filter_by_formula="Airtable Formula"
    )

    # Post a record in that table
    record = {"fields": {"field1": "value1", "field2": "value2"}}
    table.post_record(record)
    # Post several records in that table
    records = [
        {"fields": {"field1": "value1", "field2": "value2"}},
        {"fields": {"field1": "value1", "field2": "value2"}},
        {"fields": {"field1": "value1", "field2": "value2"}},
    ]
    table.post_records(records)

    # Update a record in that table
    record = {
        "id": "record id",
        "fields": {"field1": "value1", "field2": "value2"},
    }
    table.update_record()
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
    table.update_records()

    # Delete a record in that table
    record = {
        "id": "record id",
    }
    table.delete_record()
    # NOT IMPLEMENTED - Delete several records in that table
    # records = [
    #     {"id": "record id"},
    #     {"id": "record id"},
    #     {"id": "record id"},
    # ]
    # table.delete_records()


if __name__ == "__main__":
    main()

```