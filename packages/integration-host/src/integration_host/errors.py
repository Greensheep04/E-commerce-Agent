class IntegrationError(Exception):
    code = "integration_error"
    status_code = 400

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


class InvalidAdapter(IntegrationError):
    code = "invalid_adapter"


class DuplicateModule(IntegrationError):
    code = "duplicate_module"
    status_code = 409


class UnknownModule(IntegrationError):
    code = "unknown_module"
    status_code = 404


class UnknownOperation(IntegrationError):
    code = "unknown_operation"
    status_code = 404


class UnsupportedVersion(IntegrationError):
    code = "unsupported_version"
    status_code = 422


class UnsupportedOperation(IntegrationError):
    code = "unsupported_operation"
    status_code = 422


class MissingCapability(IntegrationError):
    code = "missing_capability"
    status_code = 503


class HostNotStarted(IntegrationError):
    code = "host_not_started"
    status_code = 503


class InvalidInput(IntegrationError):
    code = "invalid_input"
    status_code = 422


class InvalidOutput(IntegrationError):
    code = "invalid_output"
    status_code = 500
