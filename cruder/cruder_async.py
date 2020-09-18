from __future__ import absolute_import

import asyncio
import pandas as pd

import we.airflow.plugins.utils.snowflake as sf

from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from gspread import authorize
from typing import Iterable, Optional

from .airtable import AirtableAsync as Airtable
from .airtable import tools_async as at_tools
from .airtable.utils import Logger


logger = Logger.start(__name__)


class Cruder:
    @staticmethod
    def _fmt_col_names(
        column: str, prefix: str, style: str, abbrs: list
    ) -> str:
        """Format Column Names"""
        style = style.lower()
        col_name = ""
        if prefix:
            col_name += prefix
        for part in column.split("_"):
            if style == "upper" or (abbrs and part.lower() in abbrs):
                part = part.upper()
            elif style in ("camel", "title"):
                part = part.title()
            if style != "camel":
                col_name += part + " "
        return col_name.strip()

    @staticmethod
    async def get_csv_data(
        filepath: str,
        prefix: str = None,
        style: str = None,
        abbrs: list = None,
    ) -> pd.DataFrame:
        """
        Get CSV file as a pandas DataFrame.

        Args:
            filepath: full path to csv file.

        Kwargs:
            prefix (default=None): desired prefix
            style (default=None): "lower", "upper", "camel" or "title"
            abbrs (default=None): list of lowercase abbrs to make upper case

        Return:
            data: If succesful, pandas DataFrame
        """  # noqa: E501
        df = pd.read_csv(filepath, encoding="utf-8")

        if style:
            df.rename(
                columns=lambda x: Cruder._fmt_col_names(
                    x, prefix, style, abbrs
                ),
                inplace=True,
            )

        logger.info(f"Fetched {df.shape[0]} rows/records from {filepath}")
        return df

    @staticmethod
    async def get_snowflake_data(
        filepath: str,
        sf_hook: SnowflakeHook,
        prefix: str = None,
        style: str = None,
        abbrs: list = None,
    ) -> pd.DataFrame:
        """
        Get SQL formatted Snowflake query.

        Args:
            filepath: full path to SQL Query.
            sf_hook: Snowflake Hook

        Kwargs:
            prefix (efault=None): desired prefix
            style (default=None): "lower", "upper", "camel" or "title"
            abbrs (default=None): list of lowercase abbrs to make upper case

        Return:
            data: If succesful, pandas DataFrame
        """  # noqa: E501

        with open(filepath, "r") as fp:
            query = fp.read()

        df = sf.get_tbl_query(sf_hook=sf_hook, query=query, return_raw=False)

        if style:
            df.rename(
                columns=lambda x: Cruder._fmt_col_names(
                    x, prefix, style, abbrs
                ),
                inplace=True,
            )
        logger.info(f"Fetched {df.shape[0]} rows/records from Snowflake")
        return df

    @staticmethod
    async def convert_to_df(
        source: Optional[Iterable] = None, format: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get SQL query formatted for airtable

        Args:
            df: pandas DataFrame

        Kwargs:
            output (default="records"): "records" or "rows"

        Return:
            data: If succesful, list of records or rows
        """  # noqa: E501
        assert source in (
            "records",
            "rows",
        ), "{} is not an acceptable output type".format(source)

        if isinstance(source, dict):
            pass

        elif isinstance(source, list):
            pass

    @staticmethod
    async def convert_df(df: pd.DataFrame, output: str = "records") -> list:
        """
        Get SQL query formatted for airtable

        Args:
            df: pandas DataFrame

        Kwargs:
            output (default="records"): "records" or "rows"

        Return:
            data: If succesful, list of records or rows
        """  # noqa: E501
        assert output in (
            "records",
            "rows",
        ), "{} is not an acceptable output type".format(output)

        if output == "records":
            df = df.where(pd.notnull(df), None)
            records = [{"fields": fields} for fields in df.to_dict("records")]
            return records

        elif output == "rows":
            headers = df.columns.values.tolist()
            rows = df.values.tolist()
            rows.insert(0, headers)
            return rows

    @staticmethod
    async def crud(data: list, dest: str, **kwargs) -> None:
        """
        Post Snowflake Query to Airtable or Sheets

        Args:
            data: List of records or rows to CRUD
            dest: airtable or sheets

        Kwargs:
            credentials (``oauth2client.service_account.ServiceAccountCredentials`` or ``string``): Google Credentials Object for Service Account or Airtable API Key
            type(``string``): Type of CRUD operation ("overwrite", "update", "partial" or "append")
            target(``dict``):
                table (``string``): Airtable Table Name or ID
                wks_index (``int``): Worksheet Index
            id (``string``): Airtable Base ID or Spreadheet Key
            primary_keys (``list``): List of field names to be used as primary keys
            links (``list``, optional): List of lowercase abbrs to make upper case
                table (``string``): Name of Airtable Table to link to
                primary_key (``string``): Name of field to use as a primary key in link table
                fields (``list``): List of fields to link
            arrays (``list``, optional): list of fields to turn into arrays for Multiple Select
            overrides (``list``, optional): List of dictionaries with two entries:
                override_field (``string``): Name of field that user can override
                ref_field (``string``): Name of field (checkbox) that flags a user override
            value_input_option (``string``, default="USER_ENTERED"): Sheets input style ("RAW" or "USER_ENTERED")
        """  # noqa: E501
        if dest == "airtable":
            await Cruder._at_crud(data, **kwargs)
        elif dest == "sheets":
            Cruder._sh_crud(data, **kwargs)

    @staticmethod
    async def _at_get_linked_tables(
        base: object, records: list, link: dict
    ) -> None:
        linked_table = await base.get_table(link["table"], key="name")
        link["table"] = await linked_table.get_records()
        await at_tools.link_tables(
            records, link["table"], link["fields"], link["primary_key"],
        )

    @staticmethod
    async def _at_sort_record(
        record: dict,
        primary_keys: list,
        arrays: list,
        overrides: list,
        existing_records: list,
        existing_records_indices_by_primary_key: dict,
        post_records: list,
        update_records: list,
        existing_indices: list,
    ) -> None:
        # turn array values into lists
        if arrays:
            record = await at_tools.graft_fields(record, arrays)

        # check for existing via primary keys
        hashable_keys = await at_tools.get_primary_keys_as_hashable(  # noqa: E501
            record, primary_keys
        )
        existing_index = (
            existing_records_indices_by_primary_key.get(hashable_keys)
            if hashable_keys
            else None
        )

        # if record exists
        if existing_index is not None:
            # add index to existing indices list
            existing_indices.append(existing_index)
            existing_record = existing_records[existing_index]

            # remove overriden fields
            if overrides:
                record = await at_tools.override_record(
                    record, existing_record, overrides
                )

            # filter records to only keep new data
            record = await at_tools.filter_record(record, existing_record)

            # if new data, then append to update_records
            if record["fields"]:
                update_records.append(record)

        # else append to post_records
        else:
            post_records.append(record)

    @staticmethod
    async def _at_crud(records: list, **kwargs) -> None:
        """
        Post Snowflake Query to Airtable

        Args:
            records: List of records to CRUD

        Kwargs:
            credentials (``string``): Airtable API Key
            type(``string``): Type of CRUD operation ("overwrite", "update", "partial" or "append")
            target(``dict``):
                table (``string``): Airtable Table Name or ID
            id (``string``): Airtable Base ID
            primary_keys (``list``): List of field names to be used as primary keys
            links (``list``, optional): List of lowercase abbrs to make upper case
                table (``string``): Name of Airtable Table to link to
                primary_key (``string``): Name of field to use as a primary key in link table
                fields (``list``): List of fields to link
            arrays (``list``, optional): list of fields to turn into arrays for Multiple Select
            overrides (``list``, optional): List of dictionaries with two entries:
                override_field (``string``): Name of field that user can override
                ref_field (``string``): Name of field (checkbox) that flags a user override
        """  # noqa: E501

        api_key = kwargs.get("credentials")
        mode = kwargs.get("type")
        table_key = kwargs.get("target").get("table")
        base_id = kwargs.get("id")
        primary_keys = kwargs.get("primary_keys")
        links = kwargs.get("links") or []
        arrays = kwargs.get("arrays")
        prefix = kwargs.get("prefix")
        overrides = kwargs.get("overrides")

        # Create Airtable() instance for this base & table
        async with Airtable(api_key=api_key) as at:
            base = await at.get_base(base_id)
            await base.get_tables()
            table = [
                table
                for table in base.tables
                if table.name == table_key or table.id == table_key
            ][0]

            # Get records in that table
            existing_records = await table.get_records()

            # Get linked tables
            await asyncio.gather(
                *[
                    Cruder._at_get_linked_tables(base, records, link)
                    for link in links
                ],
                return_exceptions=False,
            )

            # If records in table
            if existing_records:
                post_records = []  # Records to post
                update_records = []  # Records to update
                delete_records = []  # Records to delete
                existing_indices = []  # Indices of records in existing_records

                existing_records_indices_by_primary_key = {}
                for i, existing_record in enumerate(existing_records):
                    hashable_keys = await at_tools.get_primary_keys_as_hashable(  # noqa: E501
                        existing_record, primary_keys
                    )
                    if hashable_keys:
                        existing_records_indices_by_primary_key[
                            hashable_keys
                        ] = i

                await asyncio.gather(
                    *[
                        Cruder._at_sort_record(
                            record,
                            primary_keys,
                            arrays,
                            overrides,
                            existing_records,
                            existing_records_indices_by_primary_key,
                            post_records,
                            update_records,
                            existing_indices,
                        )
                        for record in records
                    ],
                    return_exceptions=False,
                )

                # create new records
                await table.post_records(post_records)
                # update existing records
                await table.update_records(update_records)

                # Get dead record indices
                all_indices = set(range(len(existing_records)))
                existing_indices = set(existing_indices)
                dead_indices = all_indices - existing_indices

                if len(dead_indices) > 0:
                    # loop through records ro delete
                    for index in dead_indices:
                        # get dead record
                        dead_record = existing_records[index]

                        if mode == "overwrite":
                            delete_records.append(dead_record)
                            continue
                        if mode == "update":
                            del_field = "Delete"
                            if prefix:
                                del_field = "AUTO_Delete"

                            if not dead_record["fields"].get(del_field):
                                record = {
                                    "id": dead_record["id"],
                                    "fields": {del_field: True},
                                }
                                delete_records.append(record)

                if delete_records:
                    # delete record
                    if mode == "overwrite":
                        await table.delete_records(delete_records)
                    # flag manual deletion via 'Delete' checkbox field
                    elif mode == "update":
                        await table.update_records(delete_records)

            # If no records in table
            else:
                if arrays:
                    # turn array values into lists
                    records = [
                        await at_tools.graft_fields(record, arrays)
                        for record in records
                    ]
                await table.post_records(records)

    @staticmethod
    def _sh_crud(rows: list, **kwargs) -> None:
        """
        Post Snowflake Query to Sheets

        Args:
            rows: List of rows to CRUD

        Kwargs:
            credentials (``oauth2client.service_account.ServiceAccountCredentials``): Google Credentials Object for Service Account
            type(``string``): Type of CRUD operation ("overwrite" or "append")
            target(``dict``):
                wks_index (``int``): Worksheet Index
            id (``string``): Spreadheet Key
            value_input_option (``string``, default="USER_ENTERED"): Sheets input style ("RAW" or "USER_ENTERED")
        """  # noqa: E501
        credentials = kwargs.get("credentials")
        sh_key = kwargs.get("id")
        wks_index = kwargs.get("target").get("wks_index")
        txn_type = kwargs.get("type")
        value_input_option = kwargs.get("value_input_option") or "USER_ENTERED"

        n_of_rows = len(rows)
        n_of_columns = len(rows[0])
        row_offset = 0

        # get the work sheet
        client = authorize(credentials)
        sh = client.open_by_key(sh_key)
        wks = sh.get_worksheet(wks_index)

        # overwrite
        if txn_type == "overwrite":
            wks.resize(rows=n_of_rows, cols=n_of_columns)
            cell_list = wks.range(1, 1, n_of_rows, n_of_columns)

        # append
        elif txn_type == "append":
            rows = rows[1:]
            n_of_rows = len(rows)
            row_count = wks.row_count
            col_count = wks.col_count
            if n_of_columns > col_count:
                col_count = n_of_columns
            wks.resize(rows=row_count + n_of_rows, cols=col_count)
            wks = sh.get_worksheet(wks_index)
            cell_list = wks.range(
                row_count + 1, 1, row_count + n_of_rows, n_of_columns
            )
            row_offset = row_count

        # TODO elif txn_type in ("update", "partial")
        else:
            return

        for i, row in enumerate(rows):
            for j, item in enumerate(row):
                cell_list[i * n_of_columns + j].value = item if item else ""

        row_chunk = 1500
        max_cells = n_of_columns * row_chunk
        for i in range(int(len(cell_list) / max_cells + 1)):
            min_index = i * max_cells
            max_index = i * max_cells + max_cells - 1
            if max_index > len(cell_list):
                max_index = len(cell_list)

            sublist = cell_list[min_index:max_index]
            try:
                wks.update_cells(
                    sublist, value_input_option=value_input_option
                )
            except TypeError:
                wks.update_cells(sublist)
            start_row = int(min_index / n_of_columns) + row_offset + 1
            end_row = start_row + row_chunk - 1
            if end_row > n_of_rows + row_offset:
                end_row = n_of_rows + row_offset
            logger.info(f"Posted rows {start_row} to {end_row} to Sheets")

            if max_index == len(cell_list) - 1:
                break

        logger.info("Completed posting to Sheets")
