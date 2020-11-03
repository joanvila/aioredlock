import re

REDIS_DSN_PATTERN = r"(rediss?:\/\/)(:.+@)?(.*)"


def clean_password(details, cast=str):
    if isinstance(details, dict):
        details = {**details}
        if "password" in details:
            details["password"] = "*******"
    elif isinstance(details, list):
        details = [clean_password(x, cast=type(x)) for x in details]
    elif isinstance(details, str) and re.match(REDIS_DSN_PATTERN, details):
        details = re.sub(REDIS_DSN_PATTERN, "\\1:*******@\\3", details)
    return cast(details)
