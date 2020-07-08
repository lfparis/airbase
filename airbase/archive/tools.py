from __future__ import absolute_import

import copy
import json

from datetime import datetime, timezone
from .airtable import Airtable
from .utils import Logger


logger = Logger.start(__name__)


def elapsed_time(start_time):
    """
    """
    NOW = datetime.now(timezone.utc).replace(tzinfo=None)
    timedelta = int((NOW - start_time).total_seconds()) + 1
    return timedelta


def pretty_print(obj):
    """
    """
    print(json.dumps(obj, sort_keys=True, indent=4))


def compose_formula(value_dict):
    """

    Args:
        value_dict (``dictionary``): Keys = field_name, Values = [value, equality_or_inequality (``boolean``)]
    Returns:
        formula(``str``)
    """  # noqa

    try:
        filter_formula = "AND("
        for i, field in enumerate(value_dict):
            value = value_dict[field][0]
            equality = value_dict[field][1]
            filter_field = "{} = '{}'".format("{" + field + "}", value)
            if not equality:
                filter_field = "NOT({})".format(filter_field)
            filter_formula += filter_field
            if i < len(value_dict) - 1:
                filter_formula += ","
        filter_formula += ")"
        return filter_formula
    except KeyError:
        logger.warning("Error implementing compose_formula()")


def compose_time_formula(filter_time_field, interval):
    if filter_time_field:
        filter_time_field = "{" + filter_time_field + "}"
        return "DATETIME_DIFF(NOW(), {}, 'seconds') < {}".format(
            filter_time_field, interval
        )


def record_exists(record, table, fields):
    """
    Checks if a record already exists in a table by looking at matching fields.

    Args:
        record (``dictionary``): Record to check if exists in ``table``.
        table (``list``): List of records from airtable.
        fields (``list``): List of fields (``string``) to check for matching values.
    Returns:
        existing_record (``dictionary``): If exists. If not returns ``None``.
    """  # noqa
    filter_data = {key: record["fields"][key] for key in fields}
    for i, existing_record in enumerate(table):
        fields_found = 0
        for key, value in filter_data.items():
            other_value = existing_record["fields"].get(key)
            if other_value == value:
                fields_found += 1
        if fields_found == len(fields):
            return existing_record, i
    return None, None


def link_record(
    record, table, filters_r, filters_t, field=None, contains=False
):
    """
    Links records from another table to a record based on filter criteria.

    Args:
        record (``dictionary``): Airtable record.
        table (``list``): List of records from airtable.
        filters_r (``list``): list of fields(``string``) in ``record`` to search in ``table``.
        filters_t (``list``): matching fields(``string``) in ``table`` to search in.
    Kwargs:
        field (``string``, optional): Name of unique field to add linked records to.
        contains (``boolean``, optional): Do you want to search all fields that contain ``filter_r``?
    Returns:
        record (``dictionary``): If exists. If not returns ``None``.
    """  # noqa
    filters_r = [x.strip() for x in filters_r]
    filters_t = [x.strip() for x in filters_t]
    table = table or []

    if contains:
        contains_filters_r = []
        for filter_r in filters_r:
            for field in record["fields"]:
                if filter_r.lower() in field.lower():
                    contains_filters_r.append(field)
        filters_r = contains_filters_r

    new_record = copy.deepcopy(record)
    for filter_r in filters_r:
        link_ids = []
        for row in table:
            try:
                thetas = [
                    x.strip() for x in record["fields"][filter_r].split(",")
                ]
                for theta in thetas:
                    for filter_t in filters_t:
                        val = row["fields"][filter_t]
                        if isinstance(theta, str) and isinstance(val, str):
                            theta = theta.lower()
                            val = val.lower()
                        if theta == val:
                            link_ids.append(row["id"])
                            break
            except (KeyError, AttributeError):
                pass

        # get target field
        if not field or len(filters_r) > 1:
            field = filter_r
        # link records
        if len(link_ids) > 0:
            new_record["fields"][field] = sorted(link_ids)
        else:
            new_record["fields"][field] = None
    return new_record


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


def add_tables_to_target_data(
    ref_airtable, target_table_names, extracted_data
):
    """
    Checks whether a target table has been added to extracted data, and if not it adds it with a dummy dict.

    Args:
        ref_airtable (``object``): Airtable() of back-end trigger table
        target_table_name (``str``): Name of back-end target table.
        extracted_data (``dict``): Extracted data as produced in extract_ref_data()
    Returns:
        extracted_data (``dict``)
    """  # noqa

    for target_table_name in target_table_names:
        if target_table_name not in extracted_data["target_info"]:
            target_table = Airtable(
                ref_airtable.BASE_ID, target_table_name, log=False
            )
            target_table = target_table.get_table()

            target_info = {
                record["fields"]["Field"]: record["fields"]["Example"]
                for record in target_table
                if record["fields"]["Field"]
                in ("**BASE ID**", "**TABLE NAME**")
            }

            try:
                target_time_filter = [
                    target_record["fields"]["Field"]
                    for target_record in target_table
                    if target_record["fields"].get("Filter - Time")
                    and target_record["fields"].get("Target Table")
                    and ref_airtable.TABLE_NAME
                    in target_record["fields"].get("Target Table")
                ][0]
            except IndexError:
                target_time_filter = None

            try:
                target_value_filter = [
                    target_record["fields"]["Field"]
                    for target_record in target_table
                    if target_record["fields"].get("Filter - Value")
                    and target_record["fields"].get("Target Table")
                    == ref_airtable.TABLE_NAME
                ][0]
            except IndexError:
                target_value_filter = None

            extracted_data["target_info"][target_table_name] = {
                "base_id": target_info["**BASE ID**"],
                "table_name": target_info["**TABLE NAME**"],
                "trigger_record_id_field": None,
                "match_fields": None,
                "filter_time_fields": {
                    "target": target_time_filter,
                    "trigger": None,
                },
                "filter_value_fields": {
                    "target": target_value_filter,
                    "trigger": None,
                },
            }

    return extracted_data


def analyse_value(value):
    """
    Analyses an airtable value to see if it needs to be:
    1. flattened from a list to a string
    2. a more detailed error message composed

    Args:
        value (``obj``): any value retrieved from an airtable record field.
    Returns:
        flatten (``bool``): True if value needs to be flattened.
        error_msg (``string``): Custom error message if any.
    """
    flatten = None
    error_msg = None

    if is_record(value):
        error_msg = "{}{}{}".format(
            "If this field indicates the record id ",
            "of the target record, please check 'Flatten' and identify ",
            "the name of the target table in 'Target Table.",
        )

    elif isinstance(value, str) and len(value) == 10:
        try:
            int(value[:4])
            error_msg = "{}{}".format(
                "If this field is a date, input should be a ",
                "string in YYYY-mm-DD format, i.e. 2019-06-19.",
            )
        except ValueError:
            pass

    if isinstance(value, list) and len(value) == 1:
        flatten = True

    return flatten, error_msg


def get_fields(table):
    """
    Get all non-empty fields from an airtable table
    Args:
        table (``list``): List of records retrieved using the get_table method or formatted to match an airtable table.
    Returns:
        fields_table (``list``): List of records, where each record represents a field in the input table and is structured as follows:
            {
                "fields": {
                    "Field": <field name>,
                    "Type": <field type>,
                    "Example": <field example>,
                    "Custom Error Message": <custom message>,
                }
            }
    """  # noqa
    start_time = datetime.today()

    retrieved_fields = []
    fields_table = []

    for record in table:
        for field, value in record["fields"].items():
            if field not in retrieved_fields:
                retrieved_fields.append(field)

                flatten, error_msg = analyse_value(value)
                if flatten:
                    value = value[0]

                fields_record = {
                    "fields": {
                        "Field": field,
                        "Type": type(value).__name__,
                        "Example": str(value),
                    }
                }

                if error_msg:
                    fields_record["fields"]["Custom Error Message"] = error_msg

                fields_table.append(fields_record)

    end_time = datetime.today()
    logger.info(
        "Retrieved %s fields in: %s",
        len(retrieved_fields),
        end_time - start_time,
    )
    return fields_table


def replace_values(field, value):
    # Simplify attachement objects
    if isinstance(value, list) and isinstance(value[0], dict):
        new_value = [{"url": obj["url"]} for obj in value if "url" in obj]
    else:
        new_value = value
    return new_value


def replace_fields(record, data):
    """
    """
    new_record = {"id": record["id"], "fields": {}}

    field_names = (
        (
            data["fields"][field]["trigger"]["Field"],
            data["fields"][field]["target"]["Field"],
        )
        for field in data["fields"]
    )

    for trigger_field, target_field in field_names:
        error_msgs = [
            data["fields"][trigger_field]["trigger"].get(
                "Custom Error Message"
            ),
            data["fields"][trigger_field]["target"].get(
                "Custom Error Message"
            ),
        ]

        try:
            if data["method"] == "push":
                field_name = target_field
                field_value = record["fields"][trigger_field]
                if isinstance(field_value, list) and data["fields"][
                    trigger_field
                ]["trigger"].get("Flatten"):
                    if any(field_value):
                        field_value = ", ".join(
                            [v for v in field_value if isinstance(v, str)]
                        )
                    else:
                        field_value = None

            elif data["method"] == "pull":
                field_name = trigger_field
                field_value = record["fields"][target_field]
                if isinstance(field_value, list) and data["fields"][
                    trigger_field
                ]["target"].get("Flatten"):
                    if any(field_value):
                        field_value = ", ".join(
                            [v for v in field_value if isinstance(v, str)]
                        )
                    else:
                        field_value = None

            new_record["fields"][field_name] = replace_values(
                field_name, field_value
            )

        except KeyError:
            pass

        if not (
            data["fields"][trigger_field]["type_match"]
            or any("record id" in str(error_msg) for error_msg in error_msgs)
        ):
            if data["method"] == "push":
                field_name = trigger_field
                src_type = data["fields"][trigger_field]["trigger"]["Type"]
                tgt_type = data["fields"][trigger_field]["target"]["Type"]
                tgt_example = data["fields"][trigger_field]["target"][
                    "Example"
                ]
                error_msg = data["fields"][trigger_field]["target"].get(
                    "Custom Error Message"
                )
            elif data["method"] == "pull":
                field_name = target_field
                src_type = data["fields"][trigger_field]["target"]["Type"]
                tgt_type = data["fields"][trigger_field]["trigger"]["Type"]
                tgt_example = data["fields"][trigger_field]["trigger"][
                    "Example"
                ]
                error_msg = data["fields"][trigger_field]["trigger"].get(
                    "Custom Error Message"
                )
            message = "<{}>{}{}{}{}".format(
                field_name,
                " may need to be removed from payload,",
                " because data types don't match. ",
                "Expecting a <{}>, but received a <{}> ".format(
                    src_type, tgt_type
                ),
                "i.e. <{}>.".format(tgt_example),
            )
            if error_msg:
                message += " Clue: {}".format(error_msg)

            logger.warning(message)

    if len(new_record["fields"]) > 0:
        return new_record


def is_within_time_interval(start_time, interval, filter_field, record):
    """
    """
    if filter_field and record:
        filter_time = record["fields"].get(filter_field)
        if filter_time:
            filter_time = datetime.strptime(
                filter_time, "%Y-%m-%dT%H:%M:%S.%fZ"
            )
            return (start_time - filter_time).total_seconds() <= interval


def get_documentation(read_airtable, bridge_airtable):
    """
    Documents how an airtable table is structured, in another airtable and includes:
    base_id, table name and all non-empty fields.

    Args:
        read_airtable (``object``): Airtable() to document.
        write_airtable (``object``): Airtable() to write documentation in.
    Returns:
        None

    """  # noqa
    start_time = datetime.today()
    logger.info("Began documentation of table: %s ", read_airtable.TABLE_NAME)
    read_table = read_airtable.get_table()
    if read_table:
        read_fields = get_fields(read_table)
        read_fields.append(
            {
                "fields": {
                    "Field": "**BASE ID**",
                    "Type": type(read_airtable.BASE_ID).__name__,
                    "Example": read_airtable.BASE_ID,
                    "Custom Error Message": "This field is for reference only",
                }
            }
        )
        read_fields.append(
            {
                "fields": {
                    "Field": "**TABLE NAME**",
                    "Type": type(read_airtable.TABLE_NAME).__name__,
                    "Example": read_airtable.TABLE_NAME,
                    "Custom Error Message": "This field is for reference only",
                }
            }
        )
        crud_table(bridge_airtable, read_fields, ["Field"])

    else:
        logger.warning(
            "Please create one dummy recod in table %s\
            in order to document the table"
        )

    end_time = datetime.today()
    logger.info(
        "Finished documentation of table %s in: %s",
        read_airtable.TABLE_NAME,
        end_time - start_time,
    )


def get_method_order(method):
    """
    """
    if method == "pull":
        return "1"
    elif method == "grab":
        return "2"
    elif method == "push":
        return "3"


def extract_ref_data(ref_airtable):
    """
    The user must first establish and define a reference bridge/back-end airtable table that links tables together via one of three operations/methods:
    "pull", "push" or "grab". See <link> for more information.

    This function extracts the data from the reference bridge/back-end airtable table so compose_link_data() can then create the guide/manual/roadmap for the function link_tables()

    Args:
        ref_airtable (``Airtable``): Airtable
        
    Returns:
        extracted_data (``dict``): a dict including:
            1. linked_data (linked fields by target_table and method)
            2. trigger_info (base_id and table_name)
            3. target_info (trigger_record_id_field, match_fields, filter_fields by target_table)
    """  # noqa

    start_time = datetime.today()
    logger.info(
        "STARTED: extracting data from table: %s ", ref_airtable.TABLE_NAME
    )

    ref_table = ref_airtable.get_table()

    extracted_data = {"link_data": {}, "trigger_info": {}, "target_info": {}}

    for ref_record in ref_table:

        trigger_field_name = ref_record["fields"]["Field"]

        # trigger base id
        if trigger_field_name == "**BASE ID**":
            extracted_data["trigger_info"]["base_id"] = ref_record["fields"][
                "Example"
            ]
            continue

        # trigger table name
        elif trigger_field_name == "**TABLE NAME**":
            extracted_data["trigger_info"]["table_name"] = ref_record[
                "fields"
            ]["Example"]
            continue

        # trigger filter time field
        elif ref_record["fields"].get("Filter - Time"):
            target_tables = ref_record["fields"]["Target Table"].split(", ")
            extracted_data = add_tables_to_target_data(
                ref_airtable, target_tables, extracted_data
            )
            for target_table in target_tables:
                extracted_data["target_info"][target_table][
                    "filter_time_fields"
                ]["trigger"] = trigger_field_name
            continue

        # trigger filter value field
        elif ref_record["fields"].get("Filter - Value"):
            target_tables = ref_record["fields"]["Target Table"].split(", ")
            extracted_data = add_tables_to_target_data(
                ref_airtable, target_tables, extracted_data
            )
            for target_table in target_tables:
                extracted_data["target_info"][target_table][
                    "filter_value_fields"
                ]["trigger"] = trigger_field_name

        # unique target - record id
        elif ref_record["fields"].get("Unique Target - Record ID"):
            target_tables = ref_record["fields"]["Target Table"].split(", ")
            extracted_data = add_tables_to_target_data(
                ref_airtable, target_tables, extracted_data
            )
            for target_table in target_tables:
                extracted_data["target_info"][target_table][
                    "trigger_record_id_field"
                ] = trigger_field_name

        # generator filtering only linked fields
        linked_fields = (
            field
            for field in ref_record["fields"]
            if field[:7] in ("PUSH - ", "PULL - ", "GRAB - ")
        )

        for linked_field in linked_fields:
            linked_table_name = linked_field[7:]

            extracted_data = add_tables_to_target_data(
                ref_airtable, [linked_table_name], extracted_data
            )

            method = linked_field[:4].lower()

            order = get_method_order(linked_field[:4].lower())
            ordered_table_name = order + " - " + linked_table_name

            # create datum if linked_table_name not in link_data
            if ordered_table_name not in extracted_data["link_data"]:
                extracted_data["link_data"][ordered_table_name] = {
                    "fields": {},
                    "method": method,
                }
            target_airtable = Airtable(
                ref_airtable.BASE_ID, linked_table_name, log=False
            )
            target_record = target_airtable.get_record(
                ref_record["fields"][linked_field][0]
            )

            extracted_data["link_data"][ordered_table_name]["fields"][
                trigger_field_name
            ] = {
                "trigger": ref_record["fields"],
                "target": target_record["fields"],
                "type_match": ref_record["fields"]["Type"]
                == target_record["fields"]["Type"],
            }

            # unique target - match fields
            if ref_record["fields"].get("Unique Target - Match Field"):
                target_tables = ref_record["fields"]["Target Table"].split(
                    ", "
                )
                extracted_data = add_tables_to_target_data(
                    ref_airtable, target_tables, extracted_data
                )
                for target_table in target_tables:
                    extracted_data["target_info"][target_table][
                        "match_fields"
                    ] = {
                        "trigger": trigger_field_name,
                        "target": target_record["fields"]["Field"],
                    }

            # if field is a list force type match
            if ref_record["fields"].get("Flatten"):
                extracted_data["link_data"][ordered_table_name]["fields"][
                    trigger_field_name
                ]["type_match"] = True

    # pretty_print(extracted_data)

    end_time = datetime.today()
    logger.info(
        "FINISHED: extracting data from table: %s in: %s",
        ref_airtable.TABLE_NAME,
        end_time - start_time,
    )

    return extracted_data


def compose_link_data(extracted_data, log=True):
    """
    The user must first establish and define a reference bridge/back-end airtable table that links tables together via one of three operations/methods:
    "pull", "push" or "grab". See <link> for more information.

    This function creates a dictionary that serves as a guide/manual/roadmap for the function link_tables()

    Args:
        extracted_data (``dict``): a dict including
    
    Kwargs:
        log (``bool``, default=True): Print to logger if succesful
    
    Returns:
        link_data (``dict``): a dict detailing how to connect the tables.
        i.e.
        link_data = {
            "order - target_ref_table_name": {
                "trigger": {
                    "base_id": <trigger_base_id>,
                    "table_name": <trigger_table_name>,
                },
                "target": {
                    "base_id": <target_base_id>,
                    "table_name": <target_table_name>,
                    "ref_field": <target_ref_field> or None,
                },
                "method": <pull, push or grab>,
                "match_fields": {
                    "trigger": <trigger_field_name>,
                    "target": <target_field_name_1>
                },
                "filter_time_fields": {
                    "trigger": <trigger_field_name>,
                    "target": <target_field_name_1>
                },
                "fields": {
                    "field_1_name": {
                        "trigger": trigger_field_info_1,
                        "target": tgt_field_info_1,
                        "type_match": True,
                    },
                    "field_2_name": {
                        "trigger": trigger_field_info_2,
                        "target": tgt_field_info_2,
                        "type_match": True,
                    },
                    "field_n-1_name": {
                        "trigger": trigger_field_info_n-1,
                        "target": tgt_field_info_n-1,
                        "type_match": True,
                    },
                    "field_n_name": {
                        "trigger": trigger_field_info_n,
                        "target": tgt_field_info_n,
                        "type_match": True,
                    },
                },
            },
        }

    """  # noqa
    start_time = datetime.today()
    logger.info(
        "STARTED: getting link data of table: %s ",
        extracted_data["trigger_info"]["table_name"],
    )

    link_data = extracted_data["link_data"]
    trigger_info = extracted_data["trigger_info"]

    remove_list = []

    for ordered_table_name in link_data:
        try:
            target_info = extracted_data["target_info"][ordered_table_name[4:]]

            link_data[ordered_table_name]["trigger"] = {
                "base_id": trigger_info["base_id"],
                "table_name": trigger_info["table_name"],
                "record_id_field": target_info["trigger_record_id_field"],
            }

            link_data[ordered_table_name]["target"] = {
                "base_id": target_info["base_id"],
                "table_name": target_info["table_name"],
            }

            link_data[ordered_table_name]["match_fields"] = target_info[
                "match_fields"
            ]

            link_data[ordered_table_name]["filter_time_fields"] = target_info[
                "filter_time_fields"
            ]

            link_data[ordered_table_name]["filter_value_fields"] = target_info[
                "filter_value_fields"
            ]
        except KeyError:
            remove_list.append(ordered_table_name)
            logger.warning(
                "No matching data or unique record id found for table: %s",
                ordered_table_name[4:],
            )

    for item in remove_list:
        link_data.pop(item)

    if log:
        pretty_print(link_data)

    end_time = datetime.today()
    logger.info(
        "FINISHED: getting link data of table %s in: %s",
        extracted_data["trigger_info"]["table_name"],
        end_time - start_time,
    )

    return link_data


def _sort_data_by_method(data):
    """
    """
    if data["method"] == "push":
        origin = "trigger"
        dest = "target"

    elif data["method"] == "pull":
        origin = "target"
        dest = "trigger"

    origin_airtable = Airtable(
        data[origin]["base_id"], data[origin]["table_name"], log=False
    )
    dest_airtable = Airtable(data[dest]["base_id"], data[dest]["table_name"])
    filter_time_field = data["filter_time_fields"].get(origin)
    filter_value_field = data["filter_value_fields"].get("trigger")

    if data["match_fields"]:
        dest_match_fields = [data["match_fields"].get(dest)]
    else:
        dest_match_fields = None

    return (
        origin_airtable,
        dest_airtable,
        filter_time_field,
        filter_value_field,
        dest_match_fields,
    )


def _grab(data, interval, start_time):
    """
    """
    origin_airtable = Airtable(
        data["target"]["base_id"], data["target"]["table_name"], log=False
    )
    dest_airtable = Airtable(
        data["trigger"]["base_id"], data["trigger"]["table_name"]
    )

    filter_time_field = data["filter_time_fields"].get("trigger")

    logger.info(
        "Start: Grabbing data from table: <%s> to table: <%s>",
        origin_airtable.TABLE_NAME,
        dest_airtable.TABLE_NAME,
    )

    grab_fields = {
        field: data["fields"][field]["target"]["Field"]
        for field in data["fields"]
    }

    interval += elapsed_time(start_time)
    formula = compose_time_formula(filter_time_field, interval)

    dest_table = dest_airtable.get_table(filter_by_formula=formula) or []

    for dest_record in dest_table:
        for grab_field, origin_field in grab_fields.items():
            origin_record_ids = dest_record["fields"].get(grab_field)
            if origin_record_ids:
                origin_record_ids = origin_record_ids.split(", ")
                if is_record(origin_record_ids):
                    payload = ""
                    for i, origin_record_id in enumerate(origin_record_ids):
                        origin_record = origin_airtable.get_record(
                            origin_record_id
                        )

                        grab_value = origin_record["fields"].get(origin_field)
                        if grab_value:
                            payload += grab_value
                            if i < len(origin_record_ids) - 1:
                                payload += ", "

                    record = {
                        "id": dest_record["id"],
                        "fields": {grab_field: payload},
                    }

                    dest_airtable.update_record(record, message=record["id"])


def _post(data, interval, start_time):
    """
    """

    if data["method"] in ("push", "pull"):
        (
            origin_airtable,
            dest_airtable,
            filter_time_field,
            filter_value_field,
            dest_match_fields,
        ) = _sort_data_by_method(data)

    else:
        _grab(data, interval)
        return

    logger.info(
        "Start: %sing data from table: <%s> to table: <%s>",
        data["method"].title(),
        origin_airtable.TABLE_NAME,
        dest_airtable.TABLE_NAME,
    )

    formula = None

    if filter_time_field:
        interval += elapsed_time(start_time)
        formula_time = compose_time_formula(filter_time_field, interval)
        formula = formula_time

    if filter_value_field:
        formula_value = compose_formula({filter_value_field: ["", False]})
        formula = formula_value

    if filter_time_field and filter_value_field:
        formula = "AND({},{})".format(formula_time, formula_value)

    origin_table = origin_airtable.get_table(filter_by_formula=formula)

    if origin_table:
        origin_table = [
            replace_fields(record, data) for record in origin_table
        ]
        crud_table(
            dest_airtable,
            origin_table,
            dest_match_fields,
            update=False,
            delete=False,
        )


def _patch(data, interval, start_time):
    """
    """
    if data["method"] in ("push", "pull"):
        (
            origin_airtable,
            dest_airtable,
            filter_time_field,
            filter_value_field,
            dest_match_fields,
        ) = _sort_data_by_method(data)

    else:
        _grab(data, interval)
        return

    logger.info(
        "Start: %sing data from table: <%s> to table: <%s>",
        data["method"].title(),
        origin_airtable.TABLE_NAME,
        dest_airtable.TABLE_NAME,
    )

    formula_time = None
    formula_value = None

    if filter_time_field:
        interval += elapsed_time(start_time)
        formula_time = compose_time_formula(filter_time_field, interval)

    if filter_value_field:
        formula_value = compose_formula({filter_value_field: ["", False]})

    origin_table = origin_airtable.get_table(filter_by_formula=formula_time)

    if origin_table:
        origin_table = {record["id"]: record for record in origin_table}

        trigger_airtable = Airtable(
            data["trigger"]["base_id"],
            data["trigger"]["table_name"],
            log=False,
        )

        trigger_table = (
            trigger_airtable.get_table(filter_by_formula=formula_value) or []
        )

        # record ids generator
        record_ids = (
            (
                trigger_record["id"],
                trigger_record["fields"].get(
                    data["trigger"]["record_id_field"]
                ),
            )
            for trigger_record in trigger_table
            if trigger_record["fields"].get(data["trigger"]["record_id_field"])
        )

        for trigger_id, target_id in record_ids:
            if isinstance(target_id, list):
                target_id = target_id[0]

            if data["method"] == "push":
                origin_record_id = trigger_id
                dest_record_id = target_id

            elif data["method"] == "pull":
                origin_record_id = target_id
                dest_record_id = trigger_id

            origin_record = origin_table.get(origin_record_id)

            if origin_record:
                dest_record = dest_airtable.get_record(dest_record_id)
                origin_record = replace_fields(origin_record, data)

                if origin_record:
                    origin_record = compare_records(
                        origin_record, dest_record, "overwrite"
                    )
                    if origin_record["fields"]:
                        dest_airtable.update_record(
                            origin_record, message=dest_record_id
                        )


def link_tables(link_data, interval, start_time):
    """
    Link two tables based on relationships established in a back end airtable and defined via link_data.

    Args:
        link_data (``dict``): a dict detailing how to connect the tables. See compose_link_data() method.
    Returns:
        None

    """  # noqa

    for i in range(3):
        for table, data in link_data.items():
            if table[0] == str(i + 1):
                this_start_time = datetime.today()

                if data["method"] in ("push", "pull"):
                    if data["trigger"]["record_id_field"]:
                        _patch(data, interval, start_time)
                    else:
                        _post(data, interval, start_time)

                elif data["method"] == "grab":
                    _grab(data, interval, start_time)

                end_time = datetime.today()
                logger.info(
                    "Finished linking tables in: %s",
                    end_time - this_start_time,
                )


def crud_table(
    existing_airtable,
    new_table,
    match_fields,
    overrides=None,
    update=True,
    delete=True,
):
    """
    Create, Read, Update and Delete records for airtable table

    Args:
        existing_airtable (``Airtable``): Airtable
        new_table (``list``): list of new records. Each record should be a ``dictionary``.
        match_fields (``list``): List of field names (``string``) via which new records will match with old records.
    Kwargs:
        overrides (``list``): List of dictionaries
            Each dictionary is composed of two items: 1. The override checkbox field name, 2. The override field name
            {"ref_field": "field name", "override_field": "field name"}
        update (``bool``, default = ``True``): If True records will be updated.
        delete (``bool``, default = ``True``): If True records that are not in the new table will be deleted.
    Returns:
        None

    """  # noqa
    start_time = datetime.today()

    existing_table = existing_airtable.get_table()

    updated_indices = []
    for record in new_table:
        record_name = record["fields"][list(record["fields"].keys())[0]]
        if existing_table:
            existing_record, existing_index = record_exists(
                record, existing_table, match_fields
            )
            if existing_record:
                if update:
                    updated_indices.append(existing_index)

                    record = compare_records(
                        record,
                        existing_record,
                        "overwrite",
                        overrides=overrides,
                    )
                    if record["fields"]:
                        existing_airtable.update_record(record)
            else:
                existing_airtable.post_record(record, message=record_name)
        else:
            existing_airtable.post_record(record, message=record_name)

    if existing_table:
        updated_indices = set(updated_indices)
        all_indices = {i for i in range(len(existing_table))}
        dead_indices = all_indices - updated_indices
        if len(dead_indices) > 0:
            for index in dead_indices:
                dead_record = existing_table[index]
                record_name = dead_record["fields"][
                    list(dead_record["fields"].keys())[0]
                ]
                if delete:
                    existing_airtable.delete_record(
                        dead_record, message=record_name
                    )

    end_time = datetime.today()
    logger.info(
        "CRUDed %s records in: %s", len(new_table), end_time - start_time
    )
