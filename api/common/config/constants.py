BASE_REGEX = "^[a-zA-Z0-9_-]"
FILENAME_WITH_TIMESTAMP_REGEX = r"[a-zA-Z0-9:_\-]+.csv$"

CONTENT_ENCODING = "utf-8"

TAG_KEYS_REGEX = BASE_REGEX + "{1,128}$"
TAG_VALUES_REGEX = BASE_REGEX + "{0,256}$"

COLUMN_NAME_REGEX = "[^a-z0-9_]+"

DATE_FORMAT_REGEX = "(%[Ymd][/-]%[Ymd][/-]%[Ymd]|%[Ym][/-]%[Ym])"
