import inspect
import logging
from typing import Any, Dict

from botocore.model import ServiceModel

from localstack.aws.api import HttpResponse, RequestContext, ServiceRequestHandler, ServiceException
from localstack.aws.protocol.parser import create_parser
from localstack.aws.protocol.serializer import create_serializer

LOG = logging.getLogger(__name__)


DispatchTable = Dict[str, ServiceRequestHandler]


class Skeleton:
    service: ServiceModel
    delegate: Any
    dispatch_table: DispatchTable

    def __init__(self, service: ServiceModel, delegate: Any):
        self.service = service
        self.delegate = delegate
        self.dispatch_table: DispatchTable = dict()

        for name, obj in inspect.getmembers(delegate):
            if isinstance(obj, ServiceRequestHandler):
                self.dispatch_table[obj.operation] = obj

        self.parser = create_parser(service)
        self.serializer = create_serializer(service)

    def invoke(self, context: RequestContext) -> HttpResponse:
        parser = self.parser
        serializer = self.serializer

        # Parse the incoming HTTPRequest
        operation, instance = parser.parse(context.request)

        if operation.name not in self.dispatch_table:
            raise NotImplementedError(
                "no entry in dispatch table for %s.%s" % (self.service, operation.name)
            )

        handler = self.dispatch_table[operation.name]
        try:
            # Call the appropriate handler
            result = handler.__call__(self.delegate, context, instance)
            # Serialize result dict to an HTTPResponse and return it
            return serializer.serialize_to_response(result, operation)
        except ServiceException as e:
            return serializer.serialize_error_to_response(e, operation)
        except NotImplementedError:
            # TODO return a generic error message to avoid TF tests to break
            # TODO check with waldemar how he implemented it in moto
            pass
