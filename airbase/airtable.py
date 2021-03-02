from __future__ import absolute_import

import asyncio
import os
import urllib

from aiohttp import (
    ClientConnectionError,
    ClientConnectorError,
    ClientSession,
    ClientTimeout,
    ContentTypeError,
    TCPConnector,
    ClientResponse,
)
from json.decoder import JSONDecodeError
from typing import Any, Dict, Iterable, List, Optional  # Union

from .utils import Logger, HTTPSemaphore
from .urls import BASE_URL, META_URL


logger = Logger.start(__name__)


class BaseAirtable:
    retries = 5

    def _is_success(self, res: Optional[ClientResponse]) -> bool:
        if res and res.status >= 200 and res.status < 300:
            return True
        else:
            return False

    async def _get_data(self, res: ClientResponse):
        try:
            return await res.json(encoding="utf-8")  # dict
        # else if raw data
        except JSONDecodeError:
            return await res.text(encoding="utf-8")  # string
        except ContentTypeError:
            return await res.read()  # bytes

    async def _request(self, *args, **kwargs):
        count = 0
        while True:
            try:
                res = await self._session.request(*args, **kwargs)
                err = False
            except (
                ClientConnectionError,
                ClientConnectorError,
                asyncio.TimeoutError,
            ):
                err = True

            if err or res.status in (408, 429, 503, 504):
                delay = (2 ** count) * 0.51
                count += 1
                if count > self.retries:
                    # res may not be defined at this point
                    # res.raise_for_status()
                    return None
                else:
                    await asyncio.sleep(delay)
            else:
                return res


class Airtable(BaseAirtable):
    def __init__(self, api_key: str = None, timeout: int = 300):
        """
        Airtable class for multiple bases

        Kwargs:
            api_key (``string``): Airtable API Key.
        """
        self.api_key = api_key
        self.timeout = timeout
        self.semaphore = HTTPSemaphore(value=50, interval=1, max_calls=5)

    async def __aenter__(self):
        conn = TCPConnector(limit=100)
        timeout = ClientTimeout(total=self.timeout)
        self._session = ClientSession(
            connector=conn, headers=self.auth, timeout=timeout
        )
        return self

    async def __aexit__(self, *err):
        await self._session.close()
        self._session = None

    @property
    def api_key(self):
        if getattr(self, "_api_key", None):
            return self._api_key

    @api_key.setter
    def api_key(self, key: str):
        self._api_key = key or str(os.environ.get("AIRTABLE_API_KEY"))
        self.auth = {"Authorization": "Bearer {}".format(self.api_key)}

    async def get_bases(self) -> Optional[List]:  # noqa: F821
        async with self.semaphore:
            url = "{}/bases".format(META_URL)
            res = await self._request("get", url)

            if self._is_success(res):
                data = await self._get_data(res)
                self.bases = [
                    Base(
                        base["id"],
                        name=base["name"],
                        permission_level=base["permissionLevel"],
                        session=self._session,
                        logging_level="info",
                    )
                    for base in data["bases"]
                ]
                self._bases_by_id = {base.id: base for base in self.bases}
                self._bases_by_name = {base.name: base for base in self.bases}

            else:
                self.bases = None
        return self.bases

    async def get_base(self, value: str, key: Optional[str] = None):
        assert key in (None, "id", "name")
        if not getattr(self, "bases", None):
            if key == "id":
                return Base(
                    base_id=value, session=self._session, logging_level="info",
                )
            await self.get_bases()
        if self.bases:
            if key == "name":
                return self._bases_by_name.get(value)
            elif key == "id":
                return self._bases_by_id.get(value)
            else:
                bases = [
                    base
                    for base in self.bases
                    if base.name == value or base.id == value
                ]
                if bases:
                    return bases[0]

    async def get_enterprise_account(
        self, enterprise_account_id, logging_level="info"
    ):
        url = "{}/enterpriseAccounts/{}".format(
            META_URL, enterprise_account_id
        )
        res = await self._session.request("get", url)
        if Airtable._is_success(res):
            data = await Airtable._get_data(res)
            return Account(
                data["id"],
                data,
                session=self._session,
                logging_level=logging_level,
            )

    async def get_table(self, base_id: str, table_name: str):
        base = await self.get_base(value=base_id, key="id")
        return Table(base, table_name)


class Account(BaseAirtable):
    def __init__(
        self,
        enterprise_account_id,
        data=None,
        session=None,
        logging_level="info",
    ):
        """
        Airtable class for an Enterprise Account.
        https://airtable.com/api/enterprise

        Args:
            enterprise_account_id (``string``): ID of Entreprise Account

        Kwargs:
            logging_level (``string``, default="info"):
        """
        self.id = enterprise_account_id
        self.url = "{}/enterpriseAccounts/{}".format(META_URL, self.id)
        self._session = session
        self.logging_level = logging_level
        if data:
            self.workspace_ids = data.get("workspaceIds")
            self.user_ids = data.get("userIds")
            self.email_domains = data.get("emailDomains")
            self.created_time = data.get("createdTime")


class Base(BaseAirtable):
    def __init__(
        self,
        base_id,
        name=None,
        permission_level=None,
        session=None,
        logging_level="info",
    ):
        """
        Airtable class for one base.

        Args:
            BASE_ID (``string``): ID of target base.

        Kwargs:
            api_key (``string``): Airtable API Key.
            log (``bool``, default=True): If True it logs succesful API calls.
        """
        self.id = base_id
        self.name = name
        self.permission_level = permission_level
        self.url = "{}/bases/{}".format(META_URL, self.id)

        self._session = session
        self.semaphore = HTTPSemaphore(value=50, interval=1, max_calls=5)

        self.log = logging_level

    async def get_tables(self) -> Optional[List]:  # noqa: F821
        async with self.semaphore:
            url = "{}/tables".format(self.url)
            res = await self._request("get", url)
            if self._is_success(res):
                data = await self._get_data(res)
                self.tables = [
                    Table(
                        self,
                        table["name"],
                        table_id=table["id"],
                        primary_field_id=table["primaryFieldId"],
                        fields=table["fields"],
                        views=table["views"],
                    )
                    for table in data["tables"]
                ]
                self._tables_by_id = {table.id: table for table in self.tables}
                self._tables_by_name = {
                    table.name: table for table in self.tables
                }
            else:
                self.tables = None
        return self.tables

    async def get_table(self, value: str, key: Optional[str] = None):
        assert key in (None, "id", "name")
        if not getattr(self, "tables", None):
            if key == "name":
                return Table(self, value)
            await self.get_tables()
        if self.tables:
            if key == "name":
                return self._tables_by_name.get(value)
            elif key == "id":
                return self._tables_by_id.get(value)
            else:
                tables = [
                    table
                    for table in self.tables
                    if table.name == value or table.id == value
                ]
                if tables:
                    return tables[0]


class Table(BaseAirtable):
    def __init__(
        self,
        base: Base,
        name: str,
        table_id: str = None,
        primary_field_id: str = None,
        fields: list = None,
        views: list = None,
    ) -> None:
        """
        Airtable class for one table in one base

        Args:
            base (``string``): Base class
            name (``string``): Name of target table.

        """
        self.base = base
        self.name = name
        self.id = table_id
        self.primary_field_id = primary_field_id
        self.fields = fields
        self.views = views
        self.url = self._compose_url()
        self.primary_field_name = (
            [
                field["name"]
                for field in self.fields
                if field["id"] == self.primary_field_id
            ][0]
            if self.fields and self.primary_field_id
            else None
        )
        self._session = base._session

    @staticmethod
    def _basic_log_msg(content: Iterable) -> str:
        """
        Constructs a basic logger message
        """
        if isinstance(content, list):
            if len(content) > 1:
                plural = "s"
            else:
                plural = ""
            message = "{} record{}".format(len(content), plural)
        else:
            message = "1 record"
        return message

    def _add_record_to_url(self, record_id: str) -> str:
        """
        Composes the airtable url with a record id

        Args:
            record_id (``string``, optional): ID of target record.
        Returns:
            url (``string``): Composed url.
        """
        return f"{self.url}/{record_id}"

    def _compose_url(self) -> str:
        """
        Composes the airtable url.

        Returns:
            url (``string``): Composed url.
        """
        return f"{BASE_URL}/{self.base.id}/{urllib.parse.quote(self.name)}"

    async def _multiple(
        self, func, records: list, typecast: bool = False
    ) -> bool:
        """
        Posts/Patches/Deletes records to a table in batches of 10.

        Args:
            func (``method``): a list of records (``dictionary``) to post.
            records (``list``): a list of records (``dictionary``) to post/patch/delete.
        Kwargs:
            message (``string``, optional): Name to use for logger.
        """  # noqa: E501
        records_iter = (
            records[i : i + 10] for i in range(0, len(records), 10)
        )

        tasks = []
        for sub_list in records_iter:
            tasks.append(asyncio.create_task(func(sub_list, typecast)))
        results = await asyncio.gather(*tasks)
        if any(not r for r in results):
            return False
        else:
            return True

    async def get_record(self, record_id: str) -> dict:
        """
        Gets one record from a table.

        Args:
            record_id (``string``): ID of record.
        Returns:
            records (``list``): If succesful, a list of existing records (``dictionary``).
        """  # noqa: E501
        url = self._add_record_to_url(record_id)
        async with self.base.semaphore:
            res = await self._request("get", url)
        data = await self._get_data(res)
        if self._is_success(res):
            val = data["fields"].get(self.primary_field_name) or record_id
            logger.info(f"Fetched record: <{val}> from table: {self.name}")
            return data
        else:
            logger.error(
                f"{res.status}: Failed to get record: <{record_id}> from table: {self.name} -> {data.get('error')}"  # noqa: E501
            )
            return {}

    async def get_records(
        self,
        view: str = None,
        filter_by_fields: list = None,
        filter_by_formula: str = None,
    ) -> list:
        """
        Gets all records from a table.

        Kwargs:
            filter_by_fields (``list``, optional): list of fields(``string``) to return. Minimum 2 fields.
            filter_by_formula (``str``, optional): literally a formula.
            view (``str``, optional): view id or name.
        Returns:
            records (``list``): If succesful, a list of existing records (``dictionary``).
        """  # noqa
        params: Dict[str, Any] = {}

        # filters
        if filter_by_fields:
            params["fields"] = filter_by_fields
        if filter_by_formula:
            params["filterByFormula"] = filter_by_formula
        if view:
            params["view"] = view

        records = []
        while True:
            async with self.base.semaphore:
                res = await self._request("get", self.url, params=params)
            if not self._is_success(res):
                logger.warning(f"Table: {self.name} could not be retreived.")
                break
            data = await self._get_data(res)
            try:
                records.extend(data["records"])
            except (AttributeError, KeyError, TypeError):
                pass
            # pagination
            if "offset" in data:
                params["offset"] = data["offset"]
            else:
                break

        if len(records) != 0:
            logger.info(
                f"Fetched {len(records)} records from table: {self.name}"
            )
            self.records = records
        else:
            self.records = []
        return self.records

    async def post_record(self, record: dict, typecast: bool = False) -> bool:
        """
        Adds a record to a table.

        Args:
            record (``dictionary``): Record to post.
        Kwargs:
            message (``string``, optional): Custom logger message.
        """
        message = self._basic_log_msg(record)
        headers = {"Content-Type": "application/json"}
        data = {"fields": record["fields"]}
        if typecast:
            data["typecast"] = True
        async with self.base.semaphore:
            res = await self._request(
                "post", self.url, json=data, headers=headers
            )
        if self._is_success(res):
            logger.info(f"Posted: {message}")
            return True
        else:
            data = await self._get_data(res)
            logger.error(
                f"{res.status}: Failed to post: {message} -> '{data.get('error').get('message')}'"  # noqa:E501
            )
            return False

    async def _post_records(
        self, records: list, typecast: bool = False
    ) -> bool:
        headers = {"Content-Type": "application/json"}
        message = self._basic_log_msg(records)

        data = {
            "records": [{"fields": record["fields"]} for record in records]
        }
        if typecast:
            data["typecast"] = True
        async with self.base.semaphore:
            res = await self._session.request(
                "post", self.url, json=data, headers=headers
            )
        if self._is_success(res):
            logger.info(f"Posted: {message}")
            return True
        else:
            data = await self._get_data(res)
            logger.error(
                f"{res.status}: Failed to post: {message} -> '{data.get('error').get('message')}'"  # noqa:E501
            )
            return False

    async def post_records(
        self, records: list, typecast: bool = False
    ) -> None:
        """
        Adds records to a table in batches of 10.

        Args:
            records (``list``): a list of records (``dictionary``) to post.
        Returns:
            True if succesful
        """  # noqa: E501
        return await self._multiple(self._post_records, records, typecast)

    async def update_record(
        self, record: dict, typecast: bool = False
    ) -> bool:
        """
        Updates a record in a table.

        Args:
            record (``dictionary``): Record with updated values.
        Kwargs:
            message (``string``, optional): Name of record to use for logger.
        Returns:
            records (``list``): If succesful, a list of existing records (``dictionary``).
        """  # noqa
        message = record["fields"].get(self.primary_field_name) or record.get(
            "id"
        )
        url = self._add_record_to_url(record.get("id"))
        headers = {"Content-Type": "application/json"}
        data = {"fields": record.get("fields")}
        if typecast:
            data["typecast"] = True
        async with self.base.semaphore:
            res = await self._request("patch", url, json=data, headers=headers)
        if self._is_success(res):
            logger.info(f"Updated: {message}")
            return True
        else:
            data = await self._get_data(res)
            logger.error(
                f"{res.status}: Failed to update: {message} -> '{data.get('error').get('message')}'"  # noqa:E501
            )
            return False

    async def _update_records(
        self, records: list, typecast: bool = False
    ) -> bool:
        headers = {"Content-Type": "application/json"}
        message = self._basic_log_msg(records)
        data = {
            "records": [
                {"id": record.get("id"), "fields": record.get("fields")}
                for record in records
            ]
        }
        if typecast:
            data["typecast"] = True
        async with self.base.semaphore:
            res = await self._request(
                "patch", self.url, headers=headers, json=data
            )
        if self._is_success(res):
            logger.info(f"Updated: {message}")
            return True
        else:
            data = await self._get_data(res)
            logger.error(
                f"{res.status}: Failed to update: {message} -> '{data.get('error').get('message')}'"  # noqa:E501
            )
            return False

    async def update_records(
        self, records: list, typecast: bool = False
    ) -> bool:
        """
        Updates records in a table in batches of 10.

        Args:
            records (``list``): a list of records (``dictionary``) with updated values.
        Returns:
            True if succesful
        """  # noqa: E501
        return await self._multiple(self._update_records, records, typecast)

    async def delete_record(self, record: dict) -> bool:
        """
        Deletes a record from a table.

        Args:
            record (``dictionary``): Record to remove.
        Kwargs:
            message (``string``, optional): Custom logger message.
        """
        message = record["fields"].get(self.primary_field_name) or record.get(
            "id"
        )
        url = self._add_record_to_url(record["id"])
        async with self.base.semaphore:
            res = await self._session.request("delete", url)
        if self._is_success(res):
            logger.info(f"Deleted: {message}")
            return True
        else:
            data = await self._get_data(res)
            logger.error(
                f"{res.status}: Failed to delete: {message} -> '{data.get('error').get('message')}'"  # noqa:E501
            )
            return False

    async def _delete_records(self, records: list) -> bool:
        """
        Deletes records from a table in batches of 10.

        Args:
            records (``list``): a list of records (``dictionary``) to delete.
        Kwargs:
            message (``string``, optional): Custom logger message.
        """

        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        message = self._basic_log_msg(records)

        data = {"records[]": [record.get("id") for record in records]}
        params = urllib.parse.urlencode(data, True)

        async with self.base.semaphore:
            res = await self._request(
                "delete", self.url, params=params, headers=headers
            )
        if self._is_success(res):
            logger.info(f"Deleted: {message}")
            return True
        else:
            data = await self._get_data(res)
            logger.error(
                f"{res.status}: Failed to delete: {message} -> '{data.get('error').get('message')}'"  # noqa:E501
            )
            return False

    async def delete_records(self, records: list) -> bool:
        """
        Deletes records in a table in batches of 10.

        Args:
            records (``list``): a list of records (``dictionary``) to delete
        Returns:
            True if succesful
        """  # noqa: E501
        return await self._multiple(self._delete_records, records)
