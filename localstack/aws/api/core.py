from typing import Any, Callable, Dict, Optional, Type, TypedDict

from botocore import xform_name
from botocore.model import OperationModel, ServiceModel


class ServiceRequest(TypedDict):
    pass


ServiceResponse = Any


class ServiceException(Exception):
    pass


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
