import uuid
from typing import Literal, Optional, Union

# Add prefixes to internal database IDs to help user's differentiate between IDs for different
# resources. See https://docs.stripe.com/api for another example of this.
object_id_prefixes = {
    "Config": "cfg",
    "Job": "job",
    "Person": "p",
    "PersonRecord": "pr",
    "PotentialMatch": "pm",
    "PredictionResult": "prre",
}


def get_prefix(object_type: str) -> str:
    return object_id_prefixes[object_type]


def has_prefix(object_id: str) -> Optional[str]:
    prefix = None

    for pre in object_id_prefixes.values():
        if object_id.startswith(pre + "_"):
            prefix = pre

    return prefix


def remove_prefix(object_id: str, prefix: Optional[str] = None) -> str:
    if not prefix:
        prefix = has_prefix(object_id)

    if prefix:
        return object_id.removeprefix(prefix + "_")
    else:
        return object_id


def get_id(object_id: str) -> int:
    """Remove prefix from object ID and cast to int."""
    prefix = has_prefix(object_id)

    if not prefix:
        raise Exception("Invalid object ID: unknown object prefix")

    id = remove_prefix(object_id, prefix)

    try:
        return int(id)
    except Exception:
        raise Exception("Invalid object ID: unknown ID format")


def get_uuid(object_id: str) -> str:
    """Remove prefix from object ID."""
    prefix = has_prefix(object_id)

    if not prefix:
        raise Exception("Invalid object ID: unknown object prefix")

    id = remove_prefix(object_id, prefix)

    try:
        if not (uuid.UUID(id).version == 4):
            raise Exception("Invalid object ID: unknown ID format")
    except Exception:
        raise Exception("Invalid object ID: unknown ID format")

    # Ensure UUID is formatted with dashes
    return str(uuid.UUID(id))


def is_object_id(object_id: str, type: Literal["int", "uuid"]) -> bool:
    try:
        if type == "int":
            print(object_id, type)
            get_id(object_id)
        else:
            print(object_id, type)
            get_uuid(object_id)
    except Exception:
        return False

    return True


def get_object_id(id: Union[int, str], type: str) -> str:
    """Add object ID prefix to ID."""
    return object_id_prefixes[type] + "_" + str(id)
