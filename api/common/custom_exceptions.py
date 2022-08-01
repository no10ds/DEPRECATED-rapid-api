class BaseAppException(Exception):
    def __init__(self, message, status_code: int = 500):
        self.message = message
        self.status_code = status_code


class AWSServiceError(BaseAppException):
    def __init__(self, message):
        super().__init__(message)


class AuthorisationError(BaseAppException):
    def __init__(self, message, status_code: int = 401):
        super().__init__(message, status_code)


class UserError(BaseAppException):
    def __init__(self, message, status_code: int = 400):
        super().__init__(message, status_code)


class ConflictError(UserError):
    def __init__(self, message, status_code: int = 409):
        super().__init__(message, status_code)


class SchemaError(UserError):
    def __init__(self, message):
        super().__init__(message)


class DatasetError(UserError):
    def __init__(self, message):
        super().__init__(message)


class ClientCredentialsUnavailableError(BaseAppException):
    pass


class UserCredentialsUnavailableError(Exception):
    pass


class TableDoesNotExistError(Exception):
    pass


class TableNotCreatedError(Exception):
    pass


class CrawlerCreateFailsError(Exception):
    pass


class SchemaNotFoundError(Exception):
    pass


class CrawlerStartFailsError(Exception):
    pass


class CrawlerDeleteFailsError(Exception):
    pass


class GetCrawlerError(Exception):
    pass


class CrawlerIsNotReadyError(Exception):
    pass


class UserGroupCreationError(Exception):
    pass


class UserGroupDeletionError(Exception):
    pass


class SubjectNotFoundError(Exception):
    pass


class ProtectedDomainDoesNotExistError(Exception):
    pass
