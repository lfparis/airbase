from __future__ import absolute_import

import pandas as pd

from typing import List

from .utils import Logger


logger = Logger.start(__name__)


async def compare(
    df_1: pd.DataFrame, df_2: pd.DataFrame, primary_keys: List[str]
) -> pd.DataFrame:
    """
    Compare two pandas DataFrames and return a DataFrame with rows sorted by CRUD operation

    Args:
        df_1 (``pd.DataFrame``): DataFrame to compare (i.e. new payload)
        df_2 (``pd.DataFrame``): DataFrame to compare against (i.e. existing data)
        primary_keys(``list``): List of `str`` of the name of the primary keys to join the DataFrames

    Returns:
        pd.DataFrame
    """  # noqa: E501

    # list of headers
    df_1_headers: List[str] = df_1.columns.values.tolist()
    df_2_headers: List[str] = df_2.columns.values.tolist()

    # combined and overlapping headers for dropping columns later
    combined_headers: set = set(df_1_headers) | set(df_2_headers)
    overlapping_headers: set = set(df_1_headers) & set(df_2_headers)
    # list of overlapping_headers without primary keys
    reduced_headers_a: List[str] = list(
        overlapping_headers - set(primary_keys)
    )
    # list of combined_headers without primary keys
    reduced_headers_b: List[str] = list(combined_headers - set(primary_keys))

    # full outer join of both DataFrames on the primary_keys
    # where the left is df_1 (with suffix '_x' applied to its column names)
    #  and the right is df_2 (with suffix '_y' applied to its column names)
    combined_df: pd.DataFrame = df_1.merge(
        df_2, how="outer", on=primary_keys, indicator=True
    )

    # rows to be created are found by looking at unique rows in df_1 (left)
    create_df: pd.DataFrame = (
        combined_df.loc[lambda x: x["_merge"] == "left_only"]
        .drop(
            columns=[f"{other_header}_y" for other_header in reduced_headers_a]
        )  # df_2 columns are dropped
        .rename(
            columns={
                f"{other_header}_x": other_header
                for other_header in reduced_headers_a
            }
        )  # df_1 columns are renamed back to original (without suffix)
        .drop(columns=["_merge"])  # _merge indicator column is dropped
    )

    # rows to be deleted are found by looking at unique rows in df_2 (right)
    delete_df: pd.DataFrame = (
        combined_df.loc[lambda x: x["_merge"] == "right_only"]
        .drop(
            columns=[f"{other_header}_x" for other_header in reduced_headers_a]
        )  # df_1 columns are dropped
        .rename(
            columns={
                f"{other_header}_y": other_header
                for other_header in reduced_headers_a
            }
        )  # df_1 columns are renamed back to original (without suffix)
        .drop(columns=["_merge"])  # _merge indicator column is dropped
    )

    # rows to be updated are found by looking at rows in df_1 (left)
    # that differ from df_2 (right) minus the rows to be created (create_df)

    # full outer join of both DataFrames on no keys
    # where the left is df_1 (with suffix '_x' applied to its column names)
    #  and the right is df_2 (with suffix '_y' applied to its column names)
    df_1_unique_rows: pd.DataFrame = (
        df_1.merge(df_2, how="outer", indicator=True, sort=True)
        .loc[
            lambda x: x["_merge"] == "left_only"
        ]  # only df_1 unique rows are kept
        .drop(columns=["_merge"])  # _merge indicator column is dropped
    )

    # full outer join of both DataFrames on no primary keys
    # where the left is df_1_unique_rows
    # (with suffix '_x' applied to its column names)
    # and the right is create_df
    # (with suffix '_y' applied to its column names)
    update_df: pd.DataFrame = (
        df_1_unique_rows.merge(
            create_df, indicator=True, how="outer", on=primary_keys
        )
        .loc[
            lambda x: x["_merge"] != "both"
        ]  # only df_1_unique_rows unique rows are kept
        .drop(
            columns=[f"{other_header}_y" for other_header in reduced_headers_b]
        )  # create_df columns are dropped
        .rename(
            columns={
                f"{other_header}_x": other_header
                for other_header in reduced_headers_b
            }
        )  # df_1_unique_rows columns are renamed back to original
        .drop(columns=["_merge"])  # _merge indicator column is dropped
    )

    # insert crud_type for each
    create_df.insert(
        loc=len(df_1_headers),
        column="crud_type",
        value=["create"] * create_df.shape[0],
    )

    update_df.insert(
        loc=len(df_1_headers),
        column="crud_type",
        value=["update"] * update_df.shape[0],
    )

    delete_df.insert(
        loc=len(df_2_headers),
        column="crud_type",
        value=["delete"] * delete_df.shape[0],
    )

    return create_df.append(update_df, sort=True).append(delete_df, sort=True)


async def is_record(value):
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


async def get_primary_keys_as_hashable(record, primary_keys):
    hashable_keys = []
    for key in primary_keys:
        val = record["fields"].get(key)
        if isinstance(val, list):
            val = tuple(val)
        if val:
            hashable_keys.append(val)
    return tuple(hashable_keys) if hashable_keys else None


async def graft_fields(record, fields, separator=",", sort=True):

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


async def link_tables(
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

    # new_table = []
    for record_a in table_a:
        # new_record = copy.deepcopy(record_a)
        for field_to_link in fields_to_link_in_a:
            field_to_link = field_to_link.strip()
            val = record_a["fields"][field_to_link]
            if not val:
                continue

            keys = (x.strip() for x in val.split(","))
            record_a["fields"][field_to_link] = [
                table_b_by_primary_key.get(key)
                for key in keys
                if table_b_by_primary_key.get(key)
            ]
        # new_table.append(new_record)
    return table_a


async def combine_records(record_a, record_b, join_fields=None):
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


async def filter_record(record_a, record_b, filter_fields=None):
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
            if isinstance(record_a["fields"][key], list):
                if (
                    isinstance(record_a["fields"][key][0], dict)
                    and "url" in record_a["fields"][key][0]
                ):
                    record_a_items = set(
                        [
                            item["url"].split("/")[-1]
                            for item in record_a["fields"][key]
                        ]
                    )
                    record_b_items = set(
                        [
                            item["url"].split("/")[-1]
                            for item in record_b["fields"][key]
                        ]
                    )
                else:
                    record_a_items = set(
                        item for item in record_a["fields"][key]
                    )
                    record_b_items = set(
                        item for item in record_b["fields"][key]
                    )
                diff = record_a_items - record_b_items
                if len(diff) != 0:
                    record["fields"][key] = record_a["fields"][key]

            elif record_a["fields"][key] != record_b["fields"][key]:
                record["fields"][key] = record_a["fields"][key]

        except (KeyError, IndexError):
            if record_a["fields"][key]:
                record["fields"][key] = record_a["fields"][key]
    return record


async def override_record(record, existing_record, overrides):
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


async def compare_records(
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


async def replace_values(field, value):
    # Simplify attachement objects
    if isinstance(value, list) and isinstance(value[0], dict):
        new_value = [{"url": obj["url"]} for obj in value if "url" in obj]
    else:
        new_value = value
    return new_value
