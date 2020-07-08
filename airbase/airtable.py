from __future__ import absolute_import

import os
import urllib

from .session import Session
from .utils import Logger
from .urls import BASE_URL, META_URL


logger = Logger.start(__name__)


class Airtable(object):
    session = Session(base_url=BASE_URL)

    def __init__(self, api_key=None):
        """
        Airtable class for multiple bases

        Kwargs:
            api_key (``string``): Airtable API Key.
        """
        self.api_key = api_key

    @property
    def api_key(self):
        if getattr(self, "_api_key", None):
            return self._api_key

    @api_key.setter
    def api_key(self, key):
        self._api_key = key or str(os.environ.get("AIRTABLE_API_KEY"))
        self.auth = {"Authorization": "Bearer {}".format(self.api_key)}

    @staticmethod
    def _compose_url(base_id, table_name):
        """
        Composes the airtable url.

        Returns:
            url (``string``): Composed url.
        """
        try:
            table_name = urllib.parse.quote(table_name)
        except Exception:
            table_name = urllib.pathname2url(table_name)
        return "{}/{}/{}".format(BASE_URL, base_id, table_name)

    def get_bases(self):
        url = "{}/bases".format(META_URL)
        data, success = self.session.request("get", url, headers=self.auth)
        if success:
            self.bases = [
                Base(
                    base["id"],
                    self.auth,
                    name=base["name"],
                    permission_level=base["permissionLevel"],
                    logging_level="info",
                )
                for base in data["bases"]
            ]

    def get_base(self, base_id, logging_level="info"):
        return Base(base_id, self.auth, logging_level=logging_level)

    def get_enterprise_account(
        self, enterprise_account_id, logging_level="info"
    ):
        url = "{}/enterpriseAccounts/{}".format(
            META_URL, enterprise_account_id
        )
        data, success = self.session.request("get", url, headers=self.auth)
        if success:
            return Account(
                data["id"], self.auth, data, logging_level=logging_level
            )


class Account(Airtable):
    def __init__(
        self, enterprise_account_id, auth, data=None, logging_level="info",
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
        self.auth = auth
        self.logging_level = logging_level
        if data:
            self.workspace_ids = data.get("workspaceIds")
            self.user_ids = data.get("userIds")
            self.email_domains = data.get("emailDomains")
            self.created_time = data.get("createdTime")


class Base(Airtable):
    def __init__(
        self,
        base_id,
        auth,
        name=None,
        permission_level=None,
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
        self.url = "{}/bases/{}".format(META_URL, self.id)
        self.name = name
        self.auth = auth
        self.log = logging_level

    def get_tables(self):
        url = "{}/tables".format(self.url)
        data, success = self.session.request("get", url, headers=self.auth)
        if success:
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

    def get_table(self, table_name):
        return Table(self, table_name)


class Table(Airtable):
    def __init__(
        self,
        base,
        name,
        table_id=None,
        primary_field_id=None,
        fields=None,
        views=None,
    ):
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
        self.url = self._compose_url(self.base.id, self.name)
        self.primary_field_name = (
            [
                field["name"]
                for field in self.fields
                if field["id"] == self.primary_field_id
            ][0]
            if self.fields and self.primary_field_id
            else None
        )

    @staticmethod
    def _basic_log_msg(content):
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

    def _add_record_to_url(self, record_id):
        """
        Composes the airtable url with a record id

        Args:
            record_id (``string``, optional): ID of target record.
        Returns:
            url (``string``): Composed url.
        """
        return "{}/{}".format(self.url, record_id)

    def get_record(self, record_id):
        """
        Gets one record from a table.

        Args:
            record_id (``string``): ID of record.
        Returns:
            records (``list``): If succesful, a list of existing records (``dictionary``).
        """  # noqa
        url = self._add_record_to_url(record_id)
        data, success = self.session.request(
            "get", url, headers=self.base.auth
        )
        if success:
            logger.info("Fetched: %s from table: %s", record_id, self.name)
            return data
        else:
            logger.warning(
                "Failed to get: %s from table: %s: %s", record_id, self.name,
            )
            return

    def get_records(self, filter_by_fields=None, filter_by_formula=None):
        """
        Gets all records from a table.

        Kwargs:
            filter_by_fields (``list``, optional): list of fields(``string``) to return. Minimum 2 fields.
            filter_by_formula (``str``, optional): literally a formula.
        Returns:
            records (``list``): If succesful, a list of existing records (``dictionary``).
        """  # noqa
        params = {}

        # filters
        if filter_by_fields:
            params["fields"] = filter_by_fields
        if filter_by_formula:
            params["filterByFormula"] = filter_by_formula

        records = []
        while True:
            data, success = self.session.request(
                "get", self.url, params=params, headers=self.base.auth
            )
            if not success:
                logger.warning("Table: %s could not be retreived.", self.name)
                break
            try:
                records.extend(data["records"])
            except KeyError:
                pass
            # pagination
            if "offset" in data:
                params["offset"] = data["offset"]
            else:
                break

        if len(records) != 0:
            logger.info(
                "Fetched %s records from table: %s", len(records), self.name,
            )
            return records

    def post_record(self, record, message=None):
        """
        Adds a record to a table.

        Args:
            record (``dictionary``): Record to post.
        Kwargs:
            message (``string``, optional): Custom logger message.
        """
        if not message:
            message = self._basic_log_msg(record)
        headers = {"Content-Type": "application/json"}
        headers.update(self.base.auth)
        data = {"fields": record["fields"]}
        data, success = self.session.request(
            "post", self.url, json_data=data, headers=headers
        )
        if success:
            logger.info("Posted: %s", message)
            return True
        else:
            logger.warning("Failed to post: %s", message)

    def post_records(self, records, message=None):
        """
        Adds records to a table in batches of 10.

        Args:
            records (``list``): a list of records (``dictionary``) to post.
        Kwargs:
            message (``string``, optional): Name to use for logger.
        """
        if message:
            log_msg = message
        headers = {"Content-Type": "application/json"}
        headers.update(self.base.auth)
        records_iter = (
            records[i : i + 10] for i in range(0, len(records), 10)
        )
        for sub_list in records_iter:
            if not message:
                log_msg = self._basic_log_msg(sub_list)

            data = {
                "records": [
                    {"fields": record["fields"]} for record in sub_list
                ]
            }

            data, success = self.session.request(
                "post", self.url, json_data=data, headers=headers
            )
            if success:
                logger.info("Posted: %s", log_msg)
            else:
                logger.warning("Failed to post: %s", log_msg)

    def update_record(self, record, message=None):
        """
        Updates a record in a table.

        Args:
            record (``dictionary``): Record with updated values.
        Kwargs:
            message (``string``, optional): Name of record to use for logger.
        Returns:
            records (``list``): If succesful, a list of existing records (``dictionary``).
        """  # noqa
        try:
            if not message:
                message = record.get("id")
            url = self._add_record_to_url(record.get("id"))
            headers = {"Content-Type": "application/json"}
            headers.update(self.base.auth)
            data = {"fields": record["fields"]}
            data, success = self.session.request(
                "patch", url, headers=headers, json_data=data
            )
            if success:
                logger.info("Updated: %s ", message)
                return True
            else:
                logger.warning("Failed to update: %s", message)
        except Exception:
            logger.warning("Invalid record format provided.")

    def update_records(self, records, message=None):
        """
        Updates records in a table in batches of 10.

        Args:
            records (``list``): a list of records (``dictionary``) with updated values.
        Kwargs:
            message (``string``, optional): Custom logger message.
        """  # noqa
        try:
            if message:
                log_msg = message
            headers = {"Content-Type": "application/json"}
            headers.update(self.base.auth)
            records_iter = (
                records[i : i + 10] for i in range(0, len(records), 10)
            )
            for sub_list in records_iter:
                if not message:
                    log_msg = self._basic_log_msg(sub_list)
                data = {
                    "records": [
                        {
                            "id": record.get("id"),
                            "fields": record.get("fields"),
                        }
                        for record in sub_list
                    ]
                }
                data, success = self.session.request(
                    "patch", self.url, headers=headers, json_data=data
                )
                if success:
                    logger.info("Updated: %s ", log_msg)
                else:
                    logger.warning("Failed to update: %s", log_msg)
        except Exception:
            logger.warning("Invalid record format provided.")

    def delete_record(self, record, message=None):
        """
        Deletes a record from a table.

        Args:
            record (``dictionary``): Record to remove.
        Kwargs:
            message (``string``, optional): Custom logger message.
        """
        if not message:
            message = record.get("id")
        url = self._add_record_to_url(record["id"])
        data, success = self.session.request(
            "delete", url, headers=self.base.auth
        )
        if success:
            logger.info("Deleted: %s", message)
            return True
        else:
            logger.warning("Failed to delete: %s", message)

    def delete_records(self, records, message=None):
        """
        Deletes records from a table in batches of 10.

        Args:
            records (``list``): a list of records (``dictionary``) to delete.
        Kwargs:
            message (``string``, optional): Custom logger message.
        """
        raise NotImplementedError

        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        headers.update(self.base.auth)
        if message:
            log_msg = message
        records_iter = (
            records[i : i + 10] for i in range(0, len(records), 10)
        )
        for sub_list in records_iter:
            if not message:
                log_msg = self._basic_log_msg(sub_list)

            data = {
                "records": [{"id": record.get("id")} for record in sub_list]
            }

            data, success = self.session.request(
                "delete", self.url, urlencode=data, headers=headers
            )
            if success:
                logger.info("Deleted: %s", log_msg)
            else:
                logger.warning("Failed to delete: %s", log_msg)
