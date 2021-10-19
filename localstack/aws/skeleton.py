import inspect
from typing import Any

from botocore.model import ServiceModel

from localstack.aws.api import DispatchTable, RequestContext, ServiceRequestHandler
from localstack.aws.protocol.parser import create_parser
from localstack.aws.protocol.serializer import create_serializer


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

    def invoke(self, context: RequestContext):
        service = context.service

        parser = create_parser(service)  # TODO cache
        serializer = create_serializer(service)
        operation, instance = parser.parse(context.request)

        if operation.name not in self.dispatch_table:
            raise NotImplementedError(
                "no entry in dispatch table for %s.%s" % (service.service_name, operation.name)
            )

        handler = self.dispatch_table[operation.name]
        # Marshall responses, catch (and marshall) all errors
        try:
            result = handler.__call__(context, instance)
            return serializer.serialize(result)
            # TODO marshall the response and return it
        except:
            # TODO limit except clause
            # TODO marshall error for response
            pass
