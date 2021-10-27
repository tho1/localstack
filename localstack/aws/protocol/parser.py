import abc
import base64
import datetime
import json
import re
from collections import defaultdict
from typing import Any, List, Tuple
from urllib.parse import parse_qs
from xml.etree import ElementTree

import dateutil.parser
from botocore.model import ListShape, MapShape, OperationModel, ServiceModel, Shape, StructureShape

from localstack.aws.api import HttpRequest
from localstack.utils.common import to_str


def _text_content(func):
    # This decorator hides the difference between
    # an XML node with text or a plain string.  It's used
    # to ensure that scalar processing operates only on text
    # strings, which allows the same scalar handlers to be used
    # for XML nodes from the body and HTTP headers.
    def _get_text_content(self, request, shape, node_or_string):
        if hasattr(node_or_string, "text"):
            text = node_or_string.text
            if text is None:
                # If an XML node is empty <foo></foo>,
                # we want to parse that as an empty string,
                # not as a null/None value.
                text = ""
        else:
            text = node_or_string
        return func(self, request, shape, text)

    return _get_text_content


class RequestParser(abc.ABC):
    """Parses a request to an AWS service (OperationModel and Input-Shapes."""

    service: ServiceModel
    DEFAULT_ENCODING = "utf-8"

    def __init__(self, service: ServiceModel) -> None:
        super().__init__()
        self.service = service

    def parse(self, request: HttpRequest) -> Tuple[OperationModel, Any]:
        raise NotImplementedError

    def _parse_shape(self, request: HttpRequest, shape: Shape, node: any):
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

    def _parse_list(self, request: HttpRequest, shape: ListShape, node: list):
        # Enough implementations share list serialization that it's moved
        # up here in the base class.
        parsed = []
        member_shape = shape.member
        for item in node:
            parsed.append(self._parse_shape(request, member_shape, item))
        return parsed

    @_text_content
    def _parse_integer(self, _, __, node: str) -> int:
        return int(node)

    @_text_content
    def _parse_float(self, _, __, node: str) -> float:
        return float(node)

    @_text_content
    def _parse_blob(self, _, __, node: str) -> bytes:
        return base64.b64decode(node)

    @_text_content
    def _parse_timestamp(self, _, shape: Shape, node: str) -> datetime.datetime:
        return self._convert_str_to_timestamp(node, shape.serialization.get("timestampFormat"))

    @_text_content
    def _parse_boolean(self, _, __, node: str) -> bool:
        value = node.lower()
        if value == "true":
            return True
        if value == "false":
            return False
        raise ValueError("cannot parse boolean value %s" % node)

    @_text_content
    def _noop_parser(self, _, __, node: any):
        return node

    _parse_character = _parse_string = _noop_parser
    _parse_double = _parse_float
    _parse_long = _parse_integer

    @staticmethod
    def _get_first(node):
        if isinstance(node, (list, tuple)):
            return node[0]
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
    FLATTENED_LIST_PREFIX = "member."

    def parse(self, request: HttpRequest) -> Tuple[OperationModel, Any]:
        body = to_str(request["body"])
        instance = parse_qs(body, keep_blank_values=True)
        # The query parsing returns a list for each entry in the dict (this is how HTTP handles lists in query params).
        # However, the AWS Query format does not have any duplicates.
        # Therefore we take the first element of each entry in the dict.
        instance = {k: self._get_first(v) for k, v in instance.items()}
        operation: OperationModel = self.service.operation_model(instance["Action"])
        input_shape: StructureShape = operation.input_shape

        return operation, self._parse_shape(request, input_shape, instance)

    def _process_member(
        self, request: HttpRequest, member_name: str, member_shape: Shape, node: dict
    ):
        if isinstance(member_shape, (MapShape, ListShape, StructureShape)):
            # If we have a complex type, we filter the node and change it's keys to craft a new "context" for the
            # new hierarchy level
            sub_node = self._filter_node(member_name, node)
        else:
            # If it is a primitive type we just get the value from the dict
            sub_node = node.get(member_name)
        # The filtered node is processed and returned (or None if the sub_node is None)
        return self._parse_shape(request, member_shape, sub_node) if sub_node is not None else None

    def _parse_structure(self, request: HttpRequest, shape: StructureShape, node: dict) -> dict:
        result = dict()

        for member, member_shape in shape.members.items():
            # The key in the node is either the serialization config "name" of the shape, or the name of the member
            member_name = self._get_serialized_name(member_shape, member)
            # BUT, if it's flattened and a list, the name is defined by the list's member's name
            if member_shape.serialization.get("flattened"):
                if isinstance(member_shape, ListShape):
                    member_name = self._get_serialized_name(member_shape.member, member)
            value = self._process_member(request, member_name, member_shape, node)
            if value is not None:
                result[member] = value

        return result if len(result) > 0 else None

    def _parse_map(self, request: HttpRequest, shape: MapShape, node: dict) -> dict:
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
            key_name = f"{key_prefix}{i}.{self._get_serialized_name(shape.key, 'key')}"
            value_name = f"{key_prefix}{i}.{self._get_serialized_name(shape.value, 'value')}"

            # We process the key and value individually
            k = self._process_member(request, key_name, shape.key, node)
            v = self._process_member(request, value_name, shape.value, node)
            if k is None or v is None:
                # technically, if one exists but not the other, then that would be an invalid request
                break
            result[k] = v

        return result if len(result) > 0 else None

    def _parse_list(self, request: HttpRequest, shape: ListShape, node: dict) -> list:
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
            key_prefix += self.FLATTENED_LIST_PREFIX

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

    def _get_serialized_name(self, shape: Shape, default_name: str):
        # Returns the serialized name for the shape if it exists.
        # Otherwise it will return the passed in default_name.
        return shape.serialization.get("name", default_name)


class BaseRestRequestParser(RequestParser):
    def __init__(self, service: ServiceModel) -> None:
        super().__init__(service)
        # When parsing a request, we need to lookup the operation based on the HTTP method and URI.
        # Therefore we create a mapping when the parser is initialized.
        self.operation_lookup = defaultdict(lambda: defaultdict(OperationModel))
        for operation in service.operation_names:
            operation_model = service.operation_model(operation)
            http = operation_model.http
            if len(http) > 0:
                method = http.get("method")
                request_uri = http.get("requestUri")
                self.operation_lookup[method][request_uri] = operation_model

    def parse(self, request: HttpRequest) -> Tuple[OperationModel, Any]:
        operation = self.operation_lookup[request["method"]][request["path"]]
        # TODO extract the request ID generation to the request context
        shape = operation.input_shape
        final_parsed = {}
        self._add_modeled_parse(request, shape, final_parsed)
        return operation, final_parsed

    def _add_modeled_parse(self, response, shape, final_parsed):
        if shape is None:
            return final_parsed
        member_shapes = shape.members
        self._parse_non_payload_attrs(response, shape, member_shapes, final_parsed)
        self._parse_payload(response, shape, member_shapes, final_parsed)

    def _parse_payload(self, request, shape, member_shapes, final_parsed):
        if "payload" in shape.serialization:
            # If a payload is specified in the output shape, then only that
            # shape is used for the body payload.
            payload_member_name = shape.serialization["payload"]
            body_shape = member_shapes[payload_member_name]
            if body_shape.serialization.get("eventstream"):
                # TODO handle event stream messages
                # body = self._create_event_stream(request, body_shape)
                # final_parsed[payload_member_name] = body
                pass
            elif body_shape.type_name in ["string", "blob"]:
                # This is a stream
                body = request["body"]
                if isinstance(body, bytes):
                    body = body.decode(self.DEFAULT_ENCODING)
                final_parsed[payload_member_name] = body
            else:
                original_parsed = self._initial_body_parse(request["body"])
                final_parsed[payload_member_name] = self._parse_shape(
                    request, body_shape, original_parsed
                )
        else:
            original_parsed = self._initial_body_parse(request["body"])
            body_parsed = self._parse_shape(request, shape, original_parsed)
            final_parsed.update(body_parsed)

    def _parse_non_payload_attrs(self, response, shape, member_shapes, final_parsed):
        headers = response["headers"]
        for name in member_shapes:
            member_shape = member_shapes[name]
            location = member_shape.serialization.get("location")
            if location is None:
                continue
            elif location == "statusCode":
                final_parsed[name] = self._parse_shape(
                    response, member_shape, response["status_code"]
                )
            elif location == "headers":
                final_parsed[name] = self._parse_header_map(member_shape, headers)
            elif location == "header":
                header_name = member_shape.serialization.get("name", name)
                if header_name in headers:
                    final_parsed[name] = self._parse_shape(
                        response, member_shape, headers[header_name]
                    )

    def _parse_header_map(self, shape, headers):
        # Note that headers are case insensitive, so we .lower()
        # all header names and header prefixes.
        parsed = {}
        prefix = shape.serialization.get("name", "").lower()
        for header_name in headers:
            if header_name.lower().startswith(prefix):
                # The key name inserted into the parsed hash
                # strips off the prefix.
                name = header_name[len(prefix) :]
                parsed[name] = headers[header_name]
        return parsed

    def _initial_body_parse(self, body_contents):
        # This method should do the initial xml/json parsing of the
        # body.  We we still need to walk the parsed body in order
        # to convert types, but this method will do the first round
        # of parsing.
        raise NotImplementedError("_initial_body_parse")


class RestXMLRequestParser(BaseRestRequestParser):
    TIMESTAMP_FORMAT = "iso8601"

    def __init__(self, service_model: ServiceModel):
        super(RestXMLRequestParser, self).__init__(service_model)
        self._namespace_re = re.compile("{.*}")

    def _initial_body_parse(self, xml_string):
        if not xml_string:
            return ElementTree.Element("")
        return self._parse_xml_string_to_dom(xml_string)

    def _parse_structure(
        self, request: HttpRequest, shape: StructureShape, node: ElementTree.Element
    ) -> dict:
        parsed = {}
        members = shape.members
        xml_dict = self._build_name_to_xml_node(node)
        for member_name in members:
            member_shape = members[member_name]
            if "location" in member_shape.serialization or member_shape.serialization.get(
                "eventheader"
            ):
                # All members with locations have already been handled,
                # so we don't need to parse these members.
                continue
            xml_name = self._member_key_name(member_shape, member_name)
            member_node = xml_dict.get(xml_name)
            if member_node is not None:
                parsed[member_name] = self._parse_shape(request, member_shape, member_node)
            elif member_shape.serialization.get("xmlAttribute"):
                attribs = {}
                location_name = member_shape.serialization["name"]
                for key, value in node.attrib.items():
                    new_key = self._namespace_re.sub(location_name.split(":")[0] + ":", key)
                    attribs[new_key] = value
                if location_name in attribs:
                    parsed[member_name] = attribs[location_name]
        return parsed

    def _parse_map(self, request: HttpRequest, shape: MapShape, node: dict) -> dict:
        parsed = {}
        key_shape = shape.key
        value_shape = shape.value
        key_location_name = key_shape.serialization.get("name", "key")
        value_location_name = value_shape.serialization.get("name", "value")
        if shape.serialization.get("flattened") and not isinstance(node, list):
            node = [node]
        for keyval_node in node:
            for single_pair in keyval_node:
                # Within each <entry> there's a <key> and a <value>
                tag_name = self._node_tag(single_pair)
                if tag_name == key_location_name:
                    key_name = self._parse_shape(request, key_shape, single_pair)
                elif tag_name == value_location_name:
                    val_name = self._parse_shape(request, value_shape, single_pair)
                else:
                    # TODO either log that we suppressed an unknown tag in the request or throw a 4xx error
                    # raise ResponseParserError("Unknown tag: %s" % tag_name)
                    pass
            parsed[key_name] = val_name
        return parsed

    def _parse_list(self, request: HttpRequest, shape: ListShape, node: dict) -> list:
        # When we use _build_name_to_xml_node, repeated elements are aggregated
        # into a list.  However, we can't tell the difference between a scalar
        # value and a single element flattened list.  So before calling the
        # real _handle_list, we know that "node" should actually be a list if
        # it's flattened, and if it's not, then we make it a one element list.
        if shape.serialization.get("flattened") and not isinstance(node, list):
            node = [node]
        return super(RestXMLRequestParser, self)._parse_list(request, shape, node)

    def _node_tag(self, node):
        return self._namespace_re.sub("", node.tag)

    def _member_key_name(self, shape, member_name):
        # This method is needed because we have to special case flattened list
        # with a serialization name.  If this is the case we use the
        # locationName from the list's member shape as the key name for the
        # surrounding structure.
        if shape.type_name == "list" and shape.serialization.get("flattened"):
            list_member_serialized_name = shape.member.serialization.get("name")
            if list_member_serialized_name is not None:
                return list_member_serialized_name
        serialized_name = shape.serialization.get("name")
        if serialized_name is not None:
            return serialized_name
        return member_name

    def _parse_xml_string_to_dom(self, xml_string):
        try:
            parser = ElementTree.XMLParser(
                target=ElementTree.TreeBuilder(), encoding=self.DEFAULT_ENCODING
            )
            parser.feed(xml_string)
            root = parser.close()
        except ElementTree.ParseError as e:
            # TODO raise a specific exception here (client error, request cannot be parsed!)
            raise Exception()
            # raise ResponseParserError(
            #     "Unable to parse response (%s), "
            #     "invalid XML received. Further retries may succeed:\n%s" %
            #     (e, xml_string))
        return root

    def _build_name_to_xml_node(self, parent_node):
        # If the parent node is actually a list. We should not be trying
        # to serialize it to a dictionary. Instead, return the first element
        # in the list.
        if isinstance(parent_node, list):
            return self._build_name_to_xml_node(parent_node[0])
        xml_dict = {}
        for item in parent_node:
            key = self._node_tag(item)
            if key in xml_dict:
                # If the key already exists, the most natural
                # way to handle this is to aggregate repeated
                # keys into a single list.
                # <foo>1</foo><foo>2</foo> -> {'foo': [Node(1), Node(2)]}
                if isinstance(xml_dict[key], list):
                    xml_dict[key].append(item)
                else:
                    # Convert from a scalar to a list.
                    xml_dict[key] = [xml_dict[key], item]
            else:
                xml_dict[key] = item
        return xml_dict

    def _replace_nodes(self, parsed):
        for key, value in parsed.items():
            if list(value):
                sub_dict = self._build_name_to_xml_node(value)
                parsed[key] = self._replace_nodes(sub_dict)
            else:
                parsed[key] = value.text
        return parsed

    def _get_first_key(self, value):
        return list(value)[0]


class BaseJSONRequestParser(RequestParser):
    TIMESTAMP_FORMAT = "unixtimestamp"

    def _parse_structure(self, request, shape, value):
        final_parsed = {}
        if shape.is_document_type:
            final_parsed = value
        else:
            member_shapes = shape.members
            if value is None:
                # If the comes across the wire as "null" (None in python),
                # we should be returning this unchanged, instead of as an
                # empty dict.
                return None
            final_parsed = {}
            for member_name in member_shapes:
                member_shape = member_shapes[member_name]
                json_name = member_shape.serialization.get("name", member_name)
                raw_value = value.get(json_name)
                if raw_value is not None:
                    final_parsed[member_name] = self._parse_shape(
                        request, member_shapes[member_name], raw_value
                    )
        return final_parsed

    def _parse_map(self, request, shape, value):
        parsed = {}
        key_shape = shape.key
        value_shape = shape.value
        for key, value in value.items():
            actual_key = self._parse_shape(request, key_shape, key)
            actual_value = self._parse_shape(request, value_shape, value)
            parsed[actual_key] = actual_value
        return parsed

    def _parse_body_as_json(self, body_contents):
        if not body_contents:
            return {}
        body = body_contents.decode(self.DEFAULT_ENCODING)
        try:
            original_parsed = json.loads(body)
            return original_parsed
        except ValueError:
            # if the body cannot be parsed, include
            # the literal string as the message
            return {"message": body}

    def _parse_boolean(self, request: HttpRequest, shape: Shape, node: bool) -> None:
        return super()._noop_parser(request, shape, node)


class JSONRequestParser(BaseJSONRequestParser):
    """Response parser for the "json" protocol."""

    def parse(self, request: HttpRequest) -> Tuple[OperationModel, Any]:
        target = request["headers"]["X-Amz-Target"]
        _, operation_name = target.split(".")
        operation = self.service.operation_model(operation_name)
        shape = operation.input_shape
        final_parsed = self._do_parse(request, shape)
        return operation, final_parsed

    def _do_parse(self, request: HttpRequest, shape):
        parsed = {}
        if shape is not None:
            event_name = shape.event_stream_name
            if event_name:
                parsed = self._handle_event_stream(request, shape, event_name)
            else:
                parsed = self._handle_json_body(request, request["body"], shape)
        return parsed

    def _handle_event_stream(self, response, shape, event_name):
        # TODO handle event streams
        return

    def _handle_json_body(self, request: HttpRequest, raw_body, shape):
        # The json.loads() gives us the primitive JSON types,
        # but we need to traverse the parsed JSON data to convert
        # to richer types (blobs, timestamps, etc.
        parsed_json = self._parse_body_as_json(raw_body)
        return self._parse_shape(request, shape, parsed_json)


class RestJSONRequestParser(BaseRestRequestParser, BaseJSONRequestParser):
    def _initial_body_parse(self, body_contents):
        return self._parse_body_as_json(body_contents)


class EC2RequestParser(QueryRequestParser):
    # The EC2 protocol does not use a prefix notation for flattened lists
    FLATTENED_LIST_PREFIX = ""

    def _get_serialized_name(self, shape: Shape, default_name: str):
        # Returns the serialized name for the shape if it exists.
        # Otherwise it will return the passed in default_name.
        if "queryName" in shape.serialization:
            return shape.serialization["queryName"]
        elif "name" in shape.serialization:
            # A locationName is always capitalized
            # on input for the ec2 protocol.
            name = shape.serialization["name"]
            return name[0].upper() + name[1:]
        else:
            return default_name


def create_parser(service: ServiceModel) -> RequestParser:
    parsers = {
        "query": QueryRequestParser,
        "json": JSONRequestParser,
        "rest-json": RestJSONRequestParser,
        "rest-xml": RestXMLRequestParser,
        "ec2": EC2RequestParser,
    }

    return parsers[service.protocol](service)
