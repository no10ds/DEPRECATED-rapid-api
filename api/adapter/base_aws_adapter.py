from api.common.custom_exceptions import AWSServiceError


class BaseAWSAdapter:
    def validate_response(self, response: dict, error_message: str = None):
        error_message = (
            error_message
            or "Internal server error, please contact system administrator"
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise AWSServiceError(error_message)
