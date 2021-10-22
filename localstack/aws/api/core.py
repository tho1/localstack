from typing import Any, Callable, Dict, Optional, Type, TypedDict

from botocore import xform_name
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


class ServiceRequestHandler:
    fn: Callable
    operation: str
    expand_parameters: bool = True
    pass_context: bool = True

    def __init__(
        self,
        fn: Callable,
        operation: str,
        pass_context: bool = True,
        expand_parameters: bool = False,
    ):
        self.fn = fn
        self.operation = operation
        self.pass_context = pass_context
        self.expand_parameters = expand_parameters

    def __call__(
        self, delegate: Any, context: RequestContext, request: ServiceRequest
    ) -> Optional[ServiceResponse]:
        args = []
        kwargs = {}

        if not self.expand_parameters:
            if self.pass_context:
                args.append(context)
            args.append(request)
        else:
            if request is None:
                kwargs = {}
            else:
                kwargs = {xform_name(k): v for k, v in request.items()}
            kwargs["context"] = context

        return self.fn(delegate, *args, **kwargs)


def handler(operation: str = None, context: bool = True, expand: bool = True):
    def wrapper(fn):
        return ServiceRequestHandler(
            fn=fn, operation=operation, pass_context=context, expand_parameters=expand
        )

    return wrapper
