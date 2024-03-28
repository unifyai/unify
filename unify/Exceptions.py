class UnifyError(Exception):
    pass


class BadRequestError(UnifyError):
    pass


class AuthenticationError(UnifyError):
    pass


class PermissionDeniedError(UnifyError):
    pass


class NotFoundError(UnifyError):
    pass


class ConflictError(UnifyError):
    pass


class UnprocessableEntityError(UnifyError):
    pass


class RateLimitError(UnifyError):
    pass


class InternalServerError(UnifyError):
    pass


status_error_map = {
    400: BadRequestError,
    401: AuthenticationError,
    403: PermissionDeniedError,
    404: NotFoundError,
    409: ConflictError,
    422: UnprocessableEntityError,
    429: RateLimitError,
    500: InternalServerError,
}
