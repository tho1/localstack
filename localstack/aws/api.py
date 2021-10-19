from typing import Any, Callable, Dict, Type, TypedDict

from botocore import xform_name
from botocore.model import OperationModel, ServiceModel


class ServiceRequest(TypedDict):
    pass


class ServiceException(Exception):
    pass


Operation = Type[ServiceRequest]


class RequestContext:
    service: ServiceModel
    operation: OperationModel
    region: str
    account: str
    request: Any

    @property
    def headers(self):
        return self.request["headers"]


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

    def __call__(self, context: RequestContext, request: ServiceRequest):
        args = []
        kwargs = {}

        if not self.expand_parameters:
            if self.pass_context:
                args.append(context)
            args.append(request)
        else:
            kwargs = {xform_name(k): v for k, v in request.items()}
            kwargs["context"] = context

        return self.fn(self, *args, **kwargs)


DispatchTable = Dict[str, ServiceRequestHandler]


def handler(operation: str = None, context: bool = True, expand: bool = True):
    def wrapper(fn):
        return ServiceRequestHandler(
            fn=fn, operation=operation, pass_context=context, expand_parameters=expand
        )

    return wrapper
