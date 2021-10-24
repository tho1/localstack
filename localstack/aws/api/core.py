import functools
from typing import Any, Callable, Dict, Optional, Type, TypedDict

from botocore.model import OperationModel, ServiceModel


class ServiceRequest(TypedDict):
    pass


ServiceResponse = Any


class ServiceException(Exception):
    """
    An exception that indicates that a service error occurred.
    These exceptions, when raised during the execution of a service function, will be serialized and sent to the client.
    Do not use this exception directly (use the generated subclasses or CommonsServiceException instead).
    """

    pass


class CommonServiceException(ServiceException):
    """
    An exception which can be raised within a service during its execution, even if it is not specified (i.e. it's not
    generated based on the service specification).
    In the AWS API references, this kind of errors are usually referred to as "Common Errors", f.e.:
    https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/CommonErrors.html
    """

    def __init__(self, code: str, message: str, status_code: int = 400, sender_fault: bool = False):
        self.code = code
        self.status_code = status_code
        self.sender_fault = sender_fault
        self.message = message
        super().__init__(self.message)


Operation = Type[ServiceRequest]


class HttpRequest(TypedDict):
    path: str
    method: str
    headers: Dict[str, str]
    body: bytes


class HttpResponse(TypedDict):
    headers: Dict[str, str]
    body: bytes
    status_code: int


class RequestContext:
    service: ServiceModel
    operation: OperationModel
    region: str
    account_id: str
    request: HttpRequest


ServiceRequestHandler = Callable[[RequestContext, ServiceRequest], Optional[ServiceResponse]]


def handler(operation: str = None, context: bool = True, expand: bool = True):
    """
    Decorator that indicates that the given function is a handler
    """

    def wrapper(fn):
        @functools.wraps(fn)
        def operation_marker(*args, **kwargs):
            return fn(*args, **kwargs)

        operation_marker.operation = operation
        operation_marker.expand_parameters = expand
        operation_marker.pass_context = context

        return operation_marker

    return wrapper
