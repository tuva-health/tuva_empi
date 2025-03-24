import uuid


def is_uuidv4(value: str) -> bool:
    try:
        return uuid.UUID(value).version == 4
    except Exception:
        return False
