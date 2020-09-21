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


def is_value_acceptable(val, field_type):
    assert (
        field_type in FIELD_TYPES
    ), "{} is not an acceptable field type".format(field_type)

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
