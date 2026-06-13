"""Common error types for openaws services."""

from __future__ import annotations


class OpenAWSError(Exception):
    """Base error carrying an HTTP status code and an error code string."""

    status = 400
    code = "BadRequest"

    def __init__(self, message: str = "", *, status: int | None = None, code: str | None = None):
        super().__init__(message or self.code)
        self.message = message or self.code
        if status is not None:
            self.status = status
        if code is not None:
            self.code = code


class NotFound(OpenAWSError):
    status = 404
    code = "NotFound"


class Conflict(OpenAWSError):
    status = 409
    code = "Conflict"


class ValidationError(OpenAWSError):
    status = 400
    code = "ValidationException"
