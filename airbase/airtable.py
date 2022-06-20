from __future__ import absolute_import, annotations

import os
import re
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
from asyncio import TimeoutError, sleep
from json.decoder import JSONDecodeError
from typing import Any, Dict, Iterable, List, Optional, Union

from .decorators import chunkify
from .exceptions import AirbaseException
from .utils import Logger, HTTPSemaphore
from .urls import BASE_URL, META_URL
from .validations import validate_records


class BaseAirtable:
    retries = 5

    def __init__(
        self,
        logging_level: str = "info",
        raise_for_status: bool = False,
        verbose: bool = False,
    ) -> None:
        """
        Airtable Base Class

        Kwargs:
            raise_for_status (``string``): Raise if the response status not in 200s.
            verbose (``string``): Log stack trace

        """  # noqa: E501
        self.logging_level = logging_level
        self.logger = Logger.start(str(self), level=logging_level)
        self.raise_for_status = raise_for_status
        self.verbose = verbose

    def __str__(self):
        obj = re.search(r"(?<=\.)[\w\d_]*(?='>$)", str(self.__class__))[0]
        if getattr(self, 'name', None):
            return f"<{obj}:'{getattr(self, 'name')}' at {hex(id(self))}>"
        else:
            return f"<{obj} at {hex(id(self))}>"

    def _is_success(self, res: Optional[ClientResponse]) -> bool:
        if res and res.status >= 200 and res.status < 300:
            return True
        else:
            return False

    async def _get_data(self, res: ClientResponse) -> Union[Dict, str, bytes]:
        try:
            return await res.json(encoding="utf-8")  # dict
        # else if raw data
        except JSONDecodeError:
            return await res.text(encoding="utf-8")  # string
        except ContentTypeError:
            return await res.read()  # bytes

    async def _request(self, *args, **kwargs) -> Optional[ClientResponse]:
        count = 0
        while True:
            try:
                res = await self._session.request(*args, **kwargs)
                err = False
            except (
                ClientConnectionError,
                ClientConnectorError,
                TimeoutError,
            ):
                err = True

            if err or res.status in (408, 429, 500, 502, 503, 504):
                delay = (2 ** count) * 0.51
                count += 1
                if count > self.retries:
                    # res may not be defined at this point
                    # res.raise_for_status()
                    return None
                else:
                    await sleep(delay)
            else:
                return res

    def raise_or_log_error(self, error_msg: str) -> None:
        if self.raise_for_status:
            raise AirbaseException(error_msg)
        else:
            self.logger.error(
                error_msg, exc_info=self.verbose, stack_info=self.verbose
            )

    def get_error_message(
        self,
        method: str,
        obj: str,
        res: Union[ClientResponse, None] = None,
        data: Union[ClientResponse, None] = None,
    ) -> str:
        status = f"{res.status}: " if res else ""
        error = data.get('error') or '' if data else ''
        error_type = error.get('type') if error else None
        error_message = error.get('message') if error else None
        if error_type and error_message:
            message = f" -> <{error_type}: {error_message}>"
        elif error_type or error_message:
            message = f" -> <{error_type or error_message}>"
        else:
            message = ""

        return f"{status}Failed to {method} {obj}{message}"


class Airtable(BaseAirtable):
    def __init__(
        self, api_key: str = None, timeout: int = 300, **kwargs
    ) -> None:
        """
        Airtable class for multiple bases

        Kwargs:
            api_key (``string``): Airtable API Key.
            timeout (``int``): a ClientTimeout settings structure. 300 seconds (5min) total timeout by default
        """  # noqa: E501
        super().__init__(**kwargs)
        self.api_key = api_key
        self.timeout = timeout
        self.semaphore = HTTPSemaphore(value=50, interval=1, max_calls=5)
        self.open()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *err):
        await self.close()

    @property
    def api_key(self) -> str:
        if getattr(self, "_api_key", None):
            return self._api_key

    @api_key.setter
    def api_key(self, key: str) -> None:
        self._api_key = key or str(os.environ.get("AIRTABLE_API_KEY"))
        self.auth = {"Authorization": f"Bearer {self.api_key}"}

    def open(self) -> None:
        conn = TCPConnector(limit=100)
        timeout = ClientTimeout(total=self.timeout)
        self._session = ClientSession(
            connector=conn,
            headers=self.auth,
            timeout=timeout,
            # raise_for_status=self.raise_for_status,
        )

    async def close(self) -> None:
        await self._session.close()
        self._session = None

    async def get_bases(self) -> Optional[List[Base]]:
        async with self.semaphore:
            url = f"{META_URL}/bases"
            res = await self._request("get", url)

            data = await self._get_data(res)
            if self._is_success(res):
                self.bases = [
                    Base(
                        base["id"],
                        name=base["name"],
                        permission_level=base["permissionLevel"],
                        session=self._session,
                        logging_level=self.logging_level,
                        raise_for_status=self.raise_for_status,
                        verbose=self.verbose,
                    )
                    for base in data["bases"]
                ]
                self._bases_by_id = {base.id: base for base in self.bases}
                self._bases_by_name = {base.name: base for base in self.bases}
                self.logger.info(f"Fetched: {len(self.bases)} bases")

            else:
                error_msg = self.get_error_message(
                    method="get",
                    obj='bases',
                    res=res,
                    data=data,
                )
                self.raise_or_log_error(error_msg)
                self.bases = None
        return self.bases

    async def get_base(
        self, value: str, key: Optional[str] = None
    ) -> Optional[Base]:
        assert key in (None, "id", "name")
        if not getattr(self, "bases", None):
            if key == "id":
                self.logger.info(f"Created Base object with id: {value}")
                return Base(
                    base_id=value,
                    session=self._session,
                    logging_level=self.logging_level,
                    raise_for_status=self.raise_for_status,
                    verbose=self.verbose,
                )
            await self.get_bases()
        if self.bases:
            if key == "name":
                base = self._bases_by_name.get(value)
            elif key == "id":
                base = self._bases_by_id.get(value)
            else:
                bases = [
                    base
                    for base in self.bases
                    if base.name == value or base.id == value
                ]
                if bases:
                    base = bases[0]
            if base:
                self.logger.info(f"Fetched Base with {key if key else 'value'}: {value}")  # noqa: E501
                return base
        else:
            error_msg = self.get_error_message(
                method="get",
                obj='base',
            )
            self.raise_or_log_error(error_msg)
            return None

    async def get_enterprise_account(
        self, enterprise_account_id
    ) -> Optional[Account]:
        url = f"{META_URL}/enterpriseAccounts/{enterprise_account_id}"
        res = await self._session.request("get", url)
        data = await self._get_data(res)
        if self._is_success(res):
            self.logger.info(f"Fetched Account with id: {data.get('id')}")
            return Account(
                data["id"],
                data,
                session=self._session,
                logging_level=self.logging_level
            )
        else:
            error_msg = self.get_error_message(
                method="get",
                obj='entreprise account',
                res=res,
                data=data,
            )
            self.raise_or_log_error(error_msg)
            return None

    async def get_table(
        self, base_id: str, table_name: str
    ) -> Optional[Table]:
        base = await self.get_base(value=base_id, key="id")
        if base:
            self.logger.info(f"Created Table object with name: {table_name}")
            return Table(
                base,
                table_name,
                logging_level=self.logging_level,
                raise_for_status=self.raise_for_status,
                verbose=self.verbose,
            )
        else:
            error_msg = f"Base with id: {base_id} does not exist or invalid permissions to access this resource."  # noqa: E501
            self.raise_or_log_error(error_msg)
            return None


class Account(BaseAirtable):
    def __init__(
        self,
        enterprise_account_id,
        data=None,
        session=None,
        **kwargs
    ) -> None:
        """
        Airtable class for an Enterprise Account.
        https://airtable.com/api/enterprise

        Args:
            enterprise_account_id (``string``): ID of Entreprise Account

        Kwargs:
            logging_level (``string``, default="info"):
        """
        super().__init__(**kwargs)
        self.id = enterprise_account_id
        self.url = f"{META_URL}/enterpriseAccounts/{self.id}"
        self._session = session
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
        **kwargs,
    ) -> None:
        """
        Airtable class for one base.

        Args:
            BASE_ID (``string``): ID of target base.

        Kwargs:
            api_key (``string``): Airtable API Key.
            log (``bool``, default=True): If True it logs succesful API calls.
        """
        super().__init__(**kwargs)
        self.id = base_id
        self.name = name
        self.permission_level = permission_level
        self.url = f"{META_URL}/bases/{self.id}"

        self._session = session
        self.semaphore = HTTPSemaphore(value=50, interval=1, max_calls=5)

    async def get_tables(self) -> Optional[List[Table]]:  # noqa: F821
        async with self.semaphore:
            url = f"{self.url}/tables"
            res = await self._request("get", url)
            data = await self._get_data(res)
            if self._is_success(res):
                self.tables = [
                    Table(
                        self,
                        table["name"],
                        table_id=table["id"],
                        primary_field_id=table["primaryFieldId"],
                        fields=table["fields"],
                        views=table["views"],
                        logging_level=self.logging_level,
                        raise_for_status=self.raise_for_status,
                        verbose=self.verbose,
                    )
                    for table in data["tables"]
                ]
                self._tables_by_id = {table.id: table for table in self.tables}
                self._tables_by_name = {
                    table.name: table for table in self.tables
                }
                self.logger.info(f"Fetched: {len(self.tables)} bases")
            else:
                error_msg = self.get_error_message(
                    method="get",
                    obj='tables',
                    res=res,
                    data=data,
                )
                self.raise_or_log_error(error_msg)
                self.tables = None
        return self.tables

    async def get_table(
        self, value: str, key: Optional[str] = None
    ) -> Optional[Table]:
        assert key in (None, "id", "name")
        if not getattr(self, "tables", None):
            if key == "name":
                self.logger.info(f"Created Table object with name: {value}")
                return Table(
                    self,
                    value,
                    logging_level=self.logging_level,
                    raise_for_status=self.raise_for_status,
                    verbose=self.verbose,
                )
            await self.get_tables()
        if self.tables:
            if key == "name":
                table = self._tables_by_name.get(value)
            elif key == "id":
                table = self._tables_by_id.get(value)
            else:
                tables = [
                    table
                    for table in self.tables
                    if table.name == value or table.id == value
                ]
                if tables:
                    table = tables[0]
            if table:
                self.logger.info(f"Fetched Table with {key if key else 'value'}: {value}")  # noqa: E501
                return table
        else:
            error_msg = self.get_error_message(
                method="get",
                obj='table',
            )
            self.raise_or_log_error(error_msg)
            return None


class Table(BaseAirtable):
    def __init__(
        self,
        base: Base,
        name: str,
        table_id: str = None,
        primary_field_id: str = None,
        fields: List[Dict[str, str]] = None,
        views: List[Dict[str, str]] = None,
        **kwargs,
    ) -> None:
        """
        Airtable class for one table in one base

        Args:
            base (``string``): Base class
            name (``string``): Name of target table.

        """
        super().__init__(**kwargs)
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
        self._session: ClientSession = base._session

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
            message = f"{len(content)} record{plural}"
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

    def _get_record_primary_key_value_or_id(self, record: dict) -> str:
        if (
            record.get("fields")
            and self.primary_field_name
            and record["fields"].get(self.primary_field_name)
        ):
            return record["fields"][self.primary_field_name]
        else:
            return record.get("id")

    async def _request_record(
        self,
        method: str,
        record: Dict,
        typecast: bool = False,
        **kwargs,
    ) -> Dict:
        """
        Posts/Gets/Patches/Deletes a record in a table.

        Args:
            method (``str``): 'post', 'patch' or 'delete;
            record (``dictionary``): a recoprd to CRUD.
        Kwargs:
            typecast (``bool``, optional): if True, payload can create new options in singleSelect and multipleSelects fields
        """  # noqa: E501

        operation = method
        content_type = "application/json"
        url = self._add_record_to_url(record.get("id"))
        data = {}

        # CREATE
        if method == "post":
            data = {"fields": record["fields"]}
            url = self.url
        # READ
        elif method == "get":
            operation = "fetch"
        # UPDATE
        elif method == "patch":
            operation = "update"
            data = {"fields": record["fields"]}
            url = self._add_record_to_url(record.get("id"))
        # DELETE
        elif method == "delete":
            content_type = "application/x-www-form-urlencoded"
        else:
            raise AirbaseException("Invalid HTTP method")

        headers = {"Content-Type": content_type}

        if typecast:
            data["typecast"] = True
        async with self.base.semaphore:
            res = await self._session.request(
                method, url, json=data, headers=headers
            )
        data = await self._get_data(res)
        message = self._get_record_primary_key_value_or_id(
            data
        ) or self._basic_log_msg(data)

        if self._is_success(res):
            self.logger.info(f"{operation.title()}{'e' if operation[-1] != 'e' else ''}d: {message}")  # noqa: E501
        else:
            error_msg = self.get_error_message(
                method=operation,
                obj=message,
                res=res,
                data=data,
            )
            self.raise_or_log_error(error_msg)
        return data

    @chunkify
    async def _request_records(
        self,
        method: str,
        records: Iterable[Dict],
        typecast: bool = False,
        **kwargs,
    ) -> Dict:
        """
        Posts/Patches/Deletes records to a table in batches of 10.

        Args:
            method (``str``): 'post', 'patch' or 'delete;
            records (``list``): a list of records (``dictionary``) to CRUD.
        Kwargs:
            typecast (``bool``, optional): if True, payload can create new options in singleSelect and multipleSelects fields
        """  # noqa: E501

        operation = method
        content_type = "application/json"
        data = {}
        params = []

        # CREATE
        if method == "post":
            data = {
                "records": [{"fields": record["fields"]} for record in records]
            }
        # UPDATE
        elif method == "patch":
            operation = "update"
            data = {
                "records": [
                    {"id": record.get("id"), "fields": record.get("fields")}
                    for record in records
                ]
            }
        # DELETE
        elif method == "delete":
            content_type = "application/x-www-form-urlencoded"
            params = [("records[]", record.get("id")) for record in records]
        else:
            raise AirbaseException("Invalid HTTP method")

        headers = {"Content-Type": content_type}
        message = self._basic_log_msg(records)

        if typecast:
            data["typecast"] = True
        async with self.base.semaphore:
            res = await self._session.request(
                method, self.url, json=data, params=params, headers=headers
            )
        data = await self._get_data(res)

        if self._is_success(res):
            self.logger.info(f"{operation.title()}{'e' if operation[-1] != 'e' else ''}d: {message}")  # noqa: E501
        else:
            error_msg = self.get_error_message(
                method=operation,
                obj=message,
                res=res,
                data=data,
            )
            self.raise_or_log_error(error_msg)
        return data

    async def get_record(self, record_id: str) -> dict:
        """
        Gets one record from a table.

        Args:
            record_id (``string``): ID of record.
        Returns:
            records (``list``): If succesful, a list of existing records (``dictionary``).
        """  # noqa: E501
        return await self._request_record(
            method="get",
            record={"id": record_id},
        )

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
            data = await self._get_data(res)
            if not self._is_success(res):
                error_msg = f"{res.status}: Records for Table: {self.name} could not be retreived -> {data.get('error')}"  # noqa: E501
                self.raise_or_log_error(error_msg)
                break

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
            self.logger.info(
                f"Fetched {len(records)} records from table: {self.name}"
            )
            self.records = records
        else:
            self.records = []
        return self.records

    async def post_record(
        self, record: Dict, typecast: bool = False
    ) -> Dict:
        """
        Adds a record to a table.

        Args:
            record (``dictionary``): Record to post.
        Kwargs:
            message (``string``, optional): Custom logger message.
        """
        await validate_records(record, record_id=False)
        return await self._request_record(
            method="post",
            record=record,
            typecast=typecast,
        )

    async def post_records(
        self, records: Iterable[Dict], typecast: bool = False
    ) -> None:
        """
        Adds records to a table in batches of 10.

        Args:
            records (``list``): a list of records (``dictionary``) to post.
        Returns:
            True if succesful
        """  # noqa: E501
        await validate_records(records, record_id=False)
        return await self._request_records(
            method="post",
            records=records,
            typecast=typecast,
        )

    async def update_record(
        self, record: Dict, typecast: bool = False
    ) -> Dict:
        """
        Updates a record in a table.

        Args:
            record (``dictionary``): Record with updated values.
        Kwargs:
            message (``string``, optional): Name of record to use for logger.
        Returns:
            records (``list``): If succesful, a list of existing records (``dictionary``).
        """  # noqa
        await validate_records(record)
        return await self._request_record(
            method="patch",
            record=record,
            typecast=typecast,
        )

    async def update_records(
        self, records: Iterable[Dict], typecast: bool = False
    ) -> Dict:
        """
        Updates records in a table in batches of 10.

        Args:
            records (``list``): a list of records (``dictionary``) with updated values.
        Returns:
            True if succesful
        """  # noqa: E501
        await validate_records(records)
        return await self._request_records(
            method="patch",
            records=records,
            typecast=typecast
        )

    async def delete_record(self, record: Dict) -> Dict:
        """
        Deletes a record from a table.

        Args:
            record (``dictionary``): Record to remove.
        Kwargs:
            message (``string``, optional): Custom logger message.
        """
        await validate_records(record, fields=False)
        return await self._request_record(
            method="delete",
            record=record,
        )

    async def delete_records(self, records: Iterable[Dict]) -> Dict:
        """
        Deletes records in a table in batches of 10.

        Args:
            records (``list``): a list of records (``dictionary``) to delete
        Returns:
            True if succesful
        """  # noqa: E501
        await validate_records(records, fields=False)
        return await self._request_records(
            method="delete",
            records=records,
        )
