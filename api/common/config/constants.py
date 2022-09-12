BASE_REGEX = "^[a-zA-Z0-9_-]"
FILENAME_WITH_TIMESTAMP_REGEX = r"[a-zA-Z0-9:_\-]+.csv$"

CONTENT_ENCODING = "utf-8"

TAG_KEYS_REGEX = BASE_REGEX + "{1,128}$"
TAG_VALUES_REGEX = BASE_REGEX + "{0,256}$"

COLUMN_NAME_REGEX = "[^a-z0-9_]+"

DATE_FORMAT_REGEX = "(%[Ymd][/-]%[Ymd][/-]%[Ymd]|%[Ym][/-]%[Ym])"

EMAIL_REGEX = (
    r"([-!#-'*+/-9=?A-Z^-~]+(\.[-!#-'*+/-9=?A-Z^-~]+)*|\"([]!#-[^-~ \t]|"
    r"(\\[\t -~]))+\")@([-!#-'*+/-9=?A-Z^-~]+(\.[-!#-'*+/-9=?A-Z^-~]+)*|\[[\t -Z^-~]*])"
)

USERNAME_REGEX = "[a-zA-Z][a-zA-Z0-9@._-]{2,127}"

DEFAULT_JOB_EXPIRY_DAYS = 1
UPLOAD_JOB_EXPIRY_DAYS = 7
QUERY_JOB_EXPIRY_DAYS = 1

DATASET_QUERY_LIMIT = 100_000
