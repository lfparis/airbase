from __future__ import absolute_import

import copy

from .utils import Logger


logger = Logger.start(__name__)


def is_record(value):
    """
    Checks whether a value is a Record ID or a list of Record IDs

    Args:
        value (``obj``): any value retrieved from an airtable record field.
    Returns:
        (``bool``): True if value is Record ID or a list of Record IDs
    """
    if isinstance(value, list) and value:
        value = value[0]
    return isinstance(value, str) and value[0:3] == "rec" and len(value) == 17


def get_primary_keys_as_hashable(record, primary_keys):
    hashable_keys = []
    for key in primary_keys:
        val = record["fields"].get(key)
        if isinstance(val, list):
            val = tuple(val)
        if val:
            hashable_keys.append(val)
    return tuple(hashable_keys) if hashable_keys else None


def graft_fields(record, fields, separator=",", sort=True):

    for field in fields:
        value = record["fields"].get(field)
        if value:
            if separator in value:
                value_list = value.split(",")
                if sort:
                    value_list = value_list.sort()
            else:
                value_list = [value]
            record["fields"][field] = value_list
    return record


def link_tables(
    table_a, table_b, fields_to_link_in_a, primary_key_b,
):
    """
    Links records from another table to a record based on filter criteria.

    Args:
        table_a (``list``): List of records.
        table_b (``list``): List of records to link to.
        fields_to_link_a (``list``): list of fields(``string``) in ``table_a`` to search in ``table_b``.
        primary_key_b (``str``): key to search in ``table_b``
    Returns:
        record (``dictionary``): If exists. If not returns ``None``.
    """  # noqa: E501
    primary_key_b = primary_key_b.strip()
    table_b_by_primary_key = {
        record_b["fields"].get(primary_key_b): record_b["id"]
        for record_b in table_b
        if record_b["fields"].get(primary_key_b)
    }

    new_table = []
    for record_a in table_a:
        new_record = copy.deepcopy(record_a)
        for field_to_link in fields_to_link_in_a:
            field_to_link = field_to_link.strip()
            val = record_a["fields"][field_to_link]
            if not val:
                continue

            keys = (x.strip() for x in val.split(","))
            new_record["fields"][field_to_link] = [
                table_b_by_primary_key.get(key)
                for key in keys
                if table_b_by_primary_key.get(key)
            ]
        new_table.append(new_record)
    return new_table


def combine_records(record_a, record_b, join_fields=None):
    """
    Combines unique information from two records into 1.

    Args:
        record_a (``dictionary``): New airtable record.
        record_b (``dictionary``): Old airtable record (This will be dictate the ``id``)
    Kwargs:
        join_fields (``list``, optional): list of fields(``string``) to combine.
    Returns:
        record (``dictionary``): If succesful, the combined ``record``, else ``record_a``.
    """  # noqa
    try:
        record = {"id": record_b["id"], "fields": {}}

        if join_fields:
            keys = join_fields
        else:
            keys = record_a["fields"]
        for key in keys:
            field = record_a["fields"][key]
            if isinstance(field, list):
                field = record_a["fields"][key]
                for item in record_b["fields"][key]:
                    if item not in record_a["fields"][key]:
                        field.append(item)
            elif isinstance(field, str):
                field = (
                    record_a["fields"][key] + ", " + record_b["fields"][key]
                )
            elif isinstance(field, int) or (
                isinstance(field, float) or isinstance(field, tuple)
            ):
                field = record_a["fields"][key] + record_b["fields"][key]
            record["fields"][key] = field
        return record
    except Exception:
        return record_a


def filter_record(record_a, record_b, filter_fields=None):
    """
    Filters a record for unique information.

    Args:
        record_a (``dictionary``): New airtable record.
        record_b (``dictionary``): Old airtable record (This will be dictate the ``id``)
    Kwargs:
        filter_fields (``list``, optional): list of fields(``string``) to filter.
    Returns:
        record (``dictionary``): If succesful, the filtered ``record``, else ``record_a``.
    """  # noqa
    try:
        record = {"id": record_b["id"], "fields": {}}
        if filter_fields:
            keys = filter_fields
        else:
            keys = record_a["fields"]
    except Exception:
        logger.warning("Could not filter record.")
        return record_a

    for key in keys:
        try:
            if record_a["fields"][key] != record_b["fields"][key]:
                record["fields"][key] = record_a["fields"][key]
        except KeyError:
            if record_a["fields"][key]:
                record["fields"][key] = record_a["fields"][key]
    return record


def override_record(record, existing_record, overrides):
    """
    Removes fields from record if user has overriden them on airtable.

    Args:
        record (``dictionary``): Record from which fields will be removed if overwritten.
        existing_record (``dictionary``): Record to check for overrides.
        overrides (``list``): List of dictionaries
            Each dictionary is composed of two items: 1. The override checkbox field name, 2. The override field name
            {"ref_field": "field name", "override_field": "field name"}
    Return:
        record.
    """  # noqa
    for override in overrides:
        ref_field = override.get("ref_field")
        override_field = override.get("override_field")
        if existing_record["fields"].get(ref_field):
            record["fields"][override_field] = existing_record["fields"][
                override_field
            ]
    return record


def compare_records(
    record_a, record_b, method, overrides=None, filter_fields=None
):
    """
    Compares a record in a table.

    Args:
        record_a (``dictionary``): record to compare 
        record_b (``dictionary``): record to compare against.
        method (``string``): Either "overwrite" or "combine"
    Kwargs:
        overrides (``list``): List of dictionaries
            Each dictionary is composed of two items: 1. The override checkbox field name, 2. The override field name
            {"ref_field": "field name", "override_field": "field name"}
        filter_fields (``list``, optional): list of fields(``string``) to update.
    Returns:
        records (``list``): If succesful, a list of existing records (``dictionary``).
    """  # noqa
    try:
        if overrides:
            record = override_record(record_a, record_b, overrides)
        if method == "overwrite":
            record = filter_record(
                record_a, record_b, filter_fields=filter_fields
            )
        elif method == "combine":
            record = combine_records(
                record_a, record_b, join_fields=filter_fields
            )
        return record
    except Exception:
        logger.warning("Invalid record format provided.")


def replace_values(field, value):
    # Simplify attachement objects
    if isinstance(value, list) and isinstance(value[0], dict):
        new_value = [{"url": obj["url"]} for obj in value if "url" in obj]
    else:
        new_value = value
    return new_value
