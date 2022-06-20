from __future__ import absolute_import

from typing import Dict, Iterable, Union

from .exceptions import AirbaseException

FIELD_TYPES = (
    "singleLineText",
    "email",
    "url",
    "multilineText",
    "number",
    "percent",
    "currency",
    "singleSelect",
    "multipleSelects",
    "singleCollaborator",
    "multipleCollaborators",
    "multipleRecordLinks",
    "dateTime",
    "phoneNumber",
    "multipleAttachments",
    "checkbox",
    "formula",
    "rollup",
    "count",
    "multipleLookupValues",
    "autoNumber",
    "barcode",
)

VIEW_TYPES = ("grid", "form", "calendar", "gallery", "kanban")

PERMISSION_LEVELS = ("read", "comment", "edit", "create")


async def validate_records(
    records: Union[Iterable[Dict], Dict], record_id=True, fields=True
) -> None:
    """
    Validates a Record or Records. Raises an AirbaseException if invalid.

    Args:
        records (``dict``): a record or a a list of records
    """
    if isinstance(records, list) and records:
        for r in records:
            await validate_records(r)

    elif isinstance(records, dict):
        if record_id:
            if records.get("id"):
                if not isinstance(records["id"], str):
                    AirbaseException(
                        "Invalid Type: record['id'] must be a string."
                    )
                elif not (
                    records["id"][0:3] == "rec" and len(records["id"]) == 17
                ):
                    AirbaseException(
                        "Invalid Record ID: record['id'] must be a string in the following format: 'rec[a-zA-Z0-9]{17}'."  # noqa: E501
                    )
            else:
                AirbaseException(
                    "Invalid Record: record must include a key 'id' with its corresponding record id value."  # noqa: E501
                )

        if fields:
            if records.get("fields"):
                if not isinstance(records["fields"], dict):
                    AirbaseException(
                        "Invalid Type: record['fields'] must be a dictionary."
                    )
            else:
                AirbaseException(
                    "Invalid Record: Record must include a key 'fields' with its corresponding field names and values."  # noqa: E501
                )

    else:
        raise AirbaseException("Invalid Type: record must be a dictionary.")


def is_value_acceptable(val, field_type):
    assert (
        field_type in FIELD_TYPES
    ), f"{field_type} is not an acceptable field type"

    if isinstance(val, str) and field_type in (
        "singleLineText",
        "email",
        "url",
        "multilineText",
        "singleSelect",
    ):
        return True

    elif isinstance(val, list) and field_type in (
        "multipleSelects",
        "multipleCollaborators",
        "multipleRecordLinks",
        "multipleAttachments",
    ):
        return True

    elif isinstance(val, tuple) and field_type in (
        "multipleSelects",
        "multipleCollaborators",
        "multipleRecordLinks",
        "multipleAttachments",
    ):
        return True
