import abc
import base64
import datetime
import json
import pkgutil
from typing import Any, List, Tuple
from urllib.parse import parse_qs

import dateutil.parser
from botocore.model import ListShape, MapShape, OperationModel, ServiceModel, Shape, StructureShape

from localstack.utils.common import to_str


class Schema:
    """Loads the schema from botocore and provides access to the service shapes."""

    def __init__(self, file):
        self.schema = json.loads(pkgutil.get_data("botocore", file))

    def shape(self, name: str) -> Shape:
        """
        Looks for a shape with the given name.

        :param name: of the shape to lookup
        :return: Shape
        """
        d = self.schema["shapes"].get(name)
        if not d:
            raise ValueError("no such shape " + name)
        return Shape(name, d)


class RequestParser(abc.ABC):
    """Parses a request to an AWS service (OperationModel and Input-Shapes."""

    service: ServiceModel

    def __init__(self, service: ServiceModel) -> None:
        super().__init__()
        self.service = service

    def parse(self, request: dict) -> Tuple[OperationModel, Any]:
        raise NotImplementedError

    def _parse_shape(self, request: dict, shape: Shape, node: dict):
        location = shape.serialization.get("location")
        if location is not None:
            if location == "header":
                headers = request.get("headers")
                location_name = shape.serialization.get("locationName")
                # TODO implement proper parsing
                # https://awslabs.github.io/smithy/1.0/spec/core/http-traits.html#httpheader-trait
                # Attention: This differs from the other protocols!
                raise NotImplementedError
            elif location == "headers":
                headers = request.get("headers")
                location_name = shape.serialization.get("locationName")
                # TODO implement proper parsing
                # https://awslabs.github.io/smithy/1.0/spec/core/http-traits.html#httpprefixheaders-trait
                # Attention: This differs from the other protocols!
                raise NotImplementedError
            elif location == "querystring":
                body = to_str(request["body"])
                location_name = shape.serialization.get("locationName")
                # TODO implement proper parsing
                # https://awslabs.github.io/smithy/1.0/spec/core/http-traits.html#httpquery-trait
                # Attention: This differs from the other protocols, even the Query protocol!
                raise NotImplementedError
            elif location == "uri":
                path = to_str(request["path"])
                location_name = shape.serialization.get("locationName")
                # TODO implement proper parsing
                # https://awslabs.github.io/smithy/1.0/spec/core/http-traits.html#httplabel-trait
                # Attention: This differs from the other protocols, even the Query protocol!
                raise NotImplementedError
        else:
            # If we don't have to use a specific location, we use the node
            payload = node

        fn_name = "_parse_%s" % shape.type_name
        handler = getattr(self, fn_name, self._noop_parser)
        return handler(request, shape, payload)

    def _parse_list(self, request: dict, shape: ListShape, node: list):
        # Enough implementations share list serialization that it's moved
        # up here in the base class.
        parsed = []
        member_shape = shape.member
        for item in node:
            parsed.append(self._parse_shape(request, member_shape, item))
        return parsed

    def _parse_integer(self, _, __, node: str) -> int:
        return int(node)

    def _parse_double(self, _, __, node: str) -> float:
        return float(node)

    def _parse_blob(self, _, __, node: str) -> float:
        return base64.b64decode(node)

    def _parse_timestamp(self, _, shape: Shape, node: str) -> datetime.datetime:
        return self._convert_str_to_timestamp(node, shape.serialization.get("timestampFormat"))

    def _parse_boolean(self, _, __, node: str) -> bool:
        value = node.lower()
        if value == "true":
            return True
        if value == "false":
            return False
        raise ValueError("cannot parse boolean value %s" % node)

    def _noop_parser(self, _, __, node: any):
        return node

    @staticmethod
    def _filter_node(name: str, node: dict) -> dict:
        filtered = {k[len(name) + 1 :]: v for k, v in node.items() if k.startswith(name)}
        return filtered if len(filtered) > 0 else None

    def _timestamp_iso8601(self, date_string: str) -> datetime.datetime:
        return dateutil.parser.isoparse(date_string)

    def _timestamp_unixtimestamp(self, timestamp_string: str) -> datetime.datetime:
        return datetime.datetime.utcfromtimestamp(int(timestamp_string))

    def _timestamp_rfc822(self, datetime_string: str) -> datetime.datetime:
        from email.utils import parsedate_to_datetime

        return parsedate_to_datetime(datetime_string)

    def _convert_str_to_timestamp(self, value: str, timestamp_format=None):
        if timestamp_format is None:
            timestamp_format = self.TIMESTAMP_FORMAT
        timestamp_format = timestamp_format.lower()
        converter = getattr(self, "_timestamp_%s" % timestamp_format)
        final_value = converter(value)
        return final_value


class QueryRequestParser(RequestParser):
    TIMESTAMP_FORMAT = "iso8601"

    def parse(self, request: dict) -> Tuple[OperationModel, Any]:
        body = to_str(request["body"])
        instance = parse_qs(body, keep_blank_values=True)
        # The query parsing returns a list for each entry in the dict (this is how HTTP handles lists in query params).
        # However, the AWS Query format does not have any duplicates.
        # Therefore we take the first element of each entry in the dict.
        instance = {k: self._get_first(v) for k, v in instance.items()}
        operation: OperationModel = self.service.operation_model(instance["Action"])
        input_shape: StructureShape = operation.input_shape

        return operation, self._parse_shape(request, input_shape, instance)

    @staticmethod
    def _get_first(node):
        if isinstance(node, (list, tuple)):
            return node[0]
        return node

    def _process_member(self, request: dict, member_name: str, member_shape: Shape, node: dict):
        if isinstance(member_shape, (MapShape, ListShape, StructureShape)):
            # If we have a complex type, we filter the node and change it's keys to craft a new "context" for the
            # new hierarchy level
            sub_node = self._filter_node(member_name, node)
        else:
            # If it is a primitive type we just get the value from the dict
            sub_node = node.get(member_name)
        # The filtered node is processed and returned (or None if the sub_node is None)
        return self._parse_shape(request, member_shape, sub_node) if sub_node is not None else None

    def _parse_structure(self, request: dict, shape: StructureShape, node: dict) -> dict:
        result = dict()

        for member, member_shape in shape.members.items():
            # The key in the node is either the serialization config "name" of the shape, or the name of the member
            member_name = member_shape.serialization.get("name", member)
            # BUT, if it's flattened and a list, the name is defined by the list's member's name
            if member_shape.serialization.get("flattened"):
                if isinstance(member_shape, ListShape):
                    member_name = member_shape.member.serialization.get("name", member)
            value = self._process_member(request, member_name, member_shape, node)
            if value is not None:
                result[member] = value

        return result if len(result) > 0 else None

    def _parse_map(self, request: dict, shape: MapShape, node: dict) -> dict:
        """
        This is what the flattened key value pairs the node look like:
        {
            "Attribute.1.Name": "MyKey",
            "Attribute.1.Value": "MyValue",
            "Attribute.2.Name": ...,
            ...
        }
        This function expects an already filtered / processed node. The node dict would therefore look like:
        {
            "1.Name": "MyKey",
            "1.Value": "MyValue",
            "2.Name": ...
        }
        """
        key_prefix = ""
        # Non-flattened maps have an additional hierarchy level named "entry"
        # https://awslabs.github.io/smithy/1.0/spec/core/xml-traits.html#xmlflattened-trait
        if not shape.serialization.get("flattened"):
            key_prefix += "entry."
        result = dict()

        i = 0
        while True:
            i += 1
            # The key and value can be renamed (with their serialization config's "name").
            # By default they are called "key" and "value".
            key_name = f"{key_prefix}{i}.{shape.key.serialization.get('name', 'key')}"
            value_name = f"{key_prefix}{i}.{shape.value.serialization.get('name', 'value')}"

            # We process the key and value individually
            k = self._process_member(request, key_name, shape.key, node)
            v = self._process_member(request, value_name, shape.value, node)
            if k is None or v is None:
                # technically, if one exists but not the other, then that would be an invalid request
                break
            result[k] = v

        return result if len(result) > 0 else None

    def _parse_list(self, request: dict, shape: ListShape, node: dict) -> list:
        """
        Some actions take lists of parameters. These lists are specified using the param.[member.]n notation.
        The "member" is used if the list is not flattened.
        Values of n are integers starting from 1.
        For example, a list with two elements looks like this:
        - Flattened: &AttributeName.1=first&AttributeName.2=second
        - Non-flattened: &AttributeName.member.1=first&AttributeName.member.2=second
        This function expects an already filtered / processed node. The node dict would therefore look like:
        {
            "1": "first",
            "2": "second",
            "3": ...
        }
        """

        # Non-flattened lists have an additional hierarchy level named "member"
        # https://awslabs.github.io/smithy/1.0/spec/core/xml-traits.html#xmlflattened-trait
        key_prefix = ""
        if not shape.serialization.get("flattened"):
            key_prefix += "member."

        # We collect the list value as well as the integer indicating the list position so we can
        # later sort the list by the position, in case they attribute values are unordered
        result: List[Tuple[int, Any]] = list()

        i = 0
        while True:
            i += 1
            key_name = f"{key_prefix}{i}"
            value = self._process_member(request, key_name, shape.member, node)
            if value is None:
                break
            result.append((i, value))

        return [r[1] for r in sorted(result)] if len(result) > 0 else None


def create_parser(service: ServiceModel) -> RequestParser:
    parsers = {
        "query": QueryRequestParser,
        "json": None,  # TODO
        "rest-json": None,  # TODO
        "rest-xml": None,  # TODO
        "ec2": None,  # TODO
    }

    return parsers[service.protocol](service)
