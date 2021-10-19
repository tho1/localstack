import abc

from botocore.model import ServiceModel


class ResponseSerializer(abc.ABC):
    DEFAULT_ENCODING = "utf-8"

    def serialize_to_response(self, parameters, operation_model):
        raise NotImplementedError("serialize_to_request")


class QueryResponseSerializer(ResponseSerializer):
    pass


def create_serializer(service: ServiceModel) -> ResponseSerializer:
    serializers = {
        "query": QueryResponseSerializer,
        "json": None,  # TODO
        "rest-json": None,  # TODO
        "rest-xml": None,  # TODO
        "ec2": None,  # TODO
    }

    return serializers[service.protocol](service)
