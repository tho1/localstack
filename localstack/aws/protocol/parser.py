import abc
import json
import pkgutil
from typing import Any, List, Tuple
from urllib.parse import parse_qs

from botocore import xform_name
from botocore.model import ListShape, MapShape, OperationModel, ServiceModel, Shape, StructureShape

from localstack.aws.spec import load_service
from localstack.utils.common import to_str

data = {
    "method": "POST",
    "path": "/",
    "body": "Action=SendMessage&Version=2012-11-05&QueueUrl=http%3A%2F%2Flocalhost%3A4566%2F000000000000%2Ftf-acc-test-queue&MessageBody=%7B%22foo%22%3A+%22bared%22%7D&DelaySeconds=2",
    "headers": {
        "Remote-Addr": "127.0.0.1",
        "Host": "localhost:4566",
        "Accept-Encoding": "identity",
        "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
        "User-Agent": "aws-cli/1.20.47 Python/3.8.10 Linux/5.4.0-88-generic botocore/1.21.47",
        "X-Amz-Date": "20211009T185815Z",
        "Authorization": "AWS4-HMAC-SHA256 Credential=test/20211009/us-east-1/sqs/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=d9f93b13a07dda8cba650fba583fab92e0c72465e5e02fb56a3bb4994aefc339",
        "Content-Length": "169",
        "x-localstack-request-url": "http://localhost:4566/",
        "X-Forwarded-For": "127.0.0.1, localhost:4566",
    },
}

data = {
    "method": "POST",
    "path": "/",
    "body": "Action=SetQueueAttributes&Version=2012-11-05&QueueUrl=http%3A%2F%2Flocalhost%3A4566%2F000000000000%2Ftf-acc-test-queue&Attribute.1.Name=DelaySeconds&Attribute.1.Value=10&Attribute.2.Name=MaximumMessageSize&Attribute.2.Value=131072&Attribute.3.Name=MessageRetentionPeriod&Attribute.3.Value=259200&Attribute.4.Name=ReceiveMessageWaitTimeSeconds&Attribute.4.Value=20&Attribute.5.Name=RedrivePolicy&Attribute.5.Value=%7B%22deadLetterTargetArn%22%3A%22arn%3Aaws%3Asqs%3Aus-east-1%3A80398EXAMPLE%3AMyDeadLetterQueue%22%2C%22maxReceiveCount%22%3A%221000%22%7D&Attribute.6.Name=VisibilityTimeout&Attribute.6.Value=60",
    "headers": {
        "Remote-Addr": "127.0.0.1",
        "Host": "localhost:4566",
        "Accept-Encoding": "identity",
        "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
        "User-Agent": "aws-cli/1.20.47 Python/3.8.10 Linux/5.4.0-88-generic botocore/1.21.47",
        "X-Amz-Date": "20211009T190345Z",
        "Authorization": "AWS4-HMAC-SHA256 Credential=test/20211009/us-east-1/sqs/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=c584bc5d328b1e6cb833ad23fc278ed1c76a16b66f14ed12f144bcc6b77c7c3f",
        "Content-Length": "608",
        "x-localstack-request-url": "http://localhost:4566/",
        "X-Forwarded-For": "127.0.0.1, localhost:4566",
    },
}


# data = {
#     "method": "POST",
#     "path": "/",
#     "body": "Action=DeleteMessageBatch&Version=2012-11-05&QueueUrl=http%3A%2F%2Flocalhost%3A4566%2F000000000000%2Ftf-acc-test-queue&DeleteMessageBatchRequestEntry.1.Id=bar&DeleteMessageBatchRequestEntry.1.ReceiptHandle=foo&DeleteMessageBatchRequestEntry.2.Id=bar&DeleteMessageBatchRequestEntry.2.ReceiptHandle=foo",
#     "headers": {
#         "Remote-Addr": "127.0.0.1",
#         "Host": "localhost:4566",
#         "Accept-Encoding": "identity",
#         "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
#         "User-Agent": "Boto3/1.18.36 Python/3.8.10 Linux/5.4.0-88-generic Botocore/1.21.36",
#         "X-Amz-Date": "20211009T202120Z",
#         "Authorization": "AWS4-HMAC-SHA256 Credential=test/20211009/us-east-1/sqs/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=f01ac21fb20d97a38bd72f40c3494543230668ba41fe4fc490fc3e59c6437315",
#         "Content-Length": "300",
#         "x-localstack-request-url": "http://localhost:4566/",
#         "X-Forwarded-For": "127.0.0.1, localhost:4566"
#     }
# }


class Schema:
    def __init__(self, file):
        self.schema = json.loads(pkgutil.get_data("botocore", file))

    def shape(self, name):
        d = self.schema["shapes"].get(name)
        if not d:
            raise ValueError("no such shape " + name)
        return Shape(name, d)


class RequestParser(abc.ABC):
    service: ServiceModel

    def __init__(self, service: ServiceModel) -> None:
        super().__init__()
        self.service = service

    def parse(self, request) -> Tuple[OperationModel, Any]:
        raise NotImplementedError

    def _parse_shape(self, shape, node):
        fn_name = "_parse_%s" % shape.type_name
        handler = getattr(self, fn_name, self._noop_parser)
        return handler(shape, node)

    def _parse_list(self, shape, node):
        # Enough implementations share list serialization that it's moved
        # up here in the base class.
        parsed = []
        member_shape = shape.member
        for item in node:
            parsed.append(self._parse_shape(member_shape, item))
        return parsed

    def _parse_integer(self, _, node):
        return int(node)

    def _parse_boolean(self, _, node):
        value = node.lower()
        if value == "true":
            return True
        if value == "false":
            return False
        raise ValueError("cannot parse boolean value %s" % node)

    def _noop_parser(self, _, node):
        return node


class QueryRequestParser(RequestParser):

    def parse(self, request) -> Tuple[OperationModel, Any]:
        body = to_str(request["body"])
        instance = parse_qs(body, keep_blank_values=True)

        operation: OperationModel = self.service.operation_model(instance["Action"][0])
        input_shape: StructureShape = operation.input_shape

        return operation, self._parse_shape(input_shape, instance)

    @staticmethod
    def _get_first(node):
        if isinstance(node, (list, tuple)):
            return node[0]
        return node

    def _parse_string(self, _, node):
        return str(self._get_first(node))

    def _parse_integer(self, _, node):
        return int(self._get_first(node))

    def _parse_boolean(self, shape, node):
        return super()._parse_boolean(shape, self._get_first(node))

    def _parse_structure(self, shape, node):
        result = dict()

        for member, member_shape in shape.members.items():
            if isinstance(member_shape, (MapShape, ListShape)):
                result[member] = self._parse_shape(member_shape, node)
                continue

            if member in node:
                result[member] = self._parse_shape(member_shape, node[member])

        return result

    def _parse_map(self, shape: MapShape, node):
        """
        This is what the flattened key value pairs the node look like:
        {
            "Attribute.1.Name": ["MyKey"],
            "Attribute.1.Value": = ["MyValue"],
            "Attribute.2.Name": [...],
            ...
        }
        """
        # TODO: check what flattened means
        flattened = shape.serialization["flattened"]

        key_prefix = shape.serialization["name"]
        result = dict()

        i = 0
        while True:
            i += 1

            k_name = f"{key_prefix}.{i}.Name"
            k_value = f"{key_prefix}.{i}.Value"

            if k_name not in node or k_value not in node:
                # technically, if one exists but not the other, then that would be an invalid request
                break

            k = self._parse_shape(shape.key, node[k_name])
            v = self._parse_shape(shape.value, node[k_value])

            result[k] = v

        return result

    def _parse_list(self, shape: ListShape, node) -> List:
        """
        Some actions take lists of parameters. These lists are specified using the param.n notation. Values of n are
        integers starting from 1. For example, a parameter list with two elements looks like this:

        &AttributeName.1=first&AttributeName.2=second
        """
        key_prefix = shape.serialization["name"]

        # TODO: apparently if list shapes have enclosing dict members, they're actually encoded as maps dicts:
        #  DeleteMessageBatchRequestEntry.1.Id (i suppose this is what flattened means)

        # we collect the list value as well as the integer indicating the list position so we can
        # later sort the list by the position, in case they attribute values are unordered
        result: List[Tuple[int, Any]] = list()

        i = 0
        while True:
            i += 1

            k = f"{key_prefix}.{i}"
            if k not in node:
                break

            value = self._parse_shape(shape.member, node[k])
            result.append((i, value))

        return [r[1] for r in sorted(result)]


def create_parser(service: ServiceModel) -> RequestParser:
    parsers = {
        "query": QueryRequestParser,
        "json": None,  # TODO
        "rest-json": None,  # TODO
        "rest-xml": None,  # TODO
        "ec2": None,  # TODO
    }

    return parsers[service.protocol](service)


def main():
    request = data

    service: ServiceModel = load_service("sqs")

    parser = create_parser(service)
    operation, instance = parser.parse(request)

    # convert to function invocation
    fn = xform_name(operation.name)
    params = {xform_name(k): v for k, v in instance.items()}

    # TODO: call me now!
    print(f"{fn}(**{params})")


if __name__ == "__main__":
    main()
