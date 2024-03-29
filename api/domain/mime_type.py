from enum import Enum

from api.common.custom_exceptions import UserError


class MimeType(Enum):
    APPLICATION_JSON = "application/json"
    TEXT_CSV = "text/csv"
    BINARY = "application/octet-stream"

    @staticmethod
    def to_mimetype(mime_type: str):
        try:
            if mime_type is None or mime_type == "" or mime_type == "*/*":
                mime_type = "application/json"
            return MimeType(mime_type)
        except ValueError:
            allowed_mime_types = ", ".join([str(item.value) for item in list(MimeType)])
            raise UserError(
                f"Provided value for Accept header parameter [{mime_type}] is not supported. Supported formats: {allowed_mime_types}"
            )
