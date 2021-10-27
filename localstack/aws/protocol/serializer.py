"""
Response serializers for the different AWS service protocols.

The module contains classes that take a service's response dict, and
given an operation model, serialize the HTTP response according to the
specified output shape.

It can be seen as the counterpart to the ``parse`` module in ``botocore``
(which parses the result of these serializer). It has a lot of
similarities with the ``serialize`` module in ``botocore``, but
serves a different purpose (serializing responses instead of requests).

The different protocols have many similarities. The class hierarchy is
designed such that the serializers share as much logic as possible.
The class hierarchy looks as follows:
::
                                                ┌───────────────────┐
                                                │ResponseSerializer │
                                                └───────────────────┘
                                                   ▲      ▲      ▲
                              ┌────────────────────┘      │      └──────────────────┐
                 ┌────────────┴────────────┐ ┌────────────┴─────────────┐ ┌─────────┴────────────┐
                 │BaseXMLResponseSerializer│ │BaseRestResponseSerializer│ │JSONResponseSerializer│
                 └─────────────────────────┘ └──────────────────────────┘ └──────────────────────┘
                         ▲      ▲                    ▲           ▲                    ▲
  ┌──────────────────────┴──┐ ┌─┴────────────────────┴──┐     ┌──┴────────────────────┴──┐
  │QueryResponseSerializer  │ │RestXMLResponseSerializer│     │RestJSONResponseSerializer│
  └─────────────────────────┘ └─────────────────────────┘     └──────────────────────────┘
              ▲
   ┌──────────┴──────────┐
   │EC2ResponseSerializer│
   └─────────────────────┘
::

The ``ResponseSerializer`` contains the logic that is used among all
the different protocols (``query``, ``json``, ``rest-json``, ``rest-xml``).
The protocols relate to each other in the following ways:
* The ``query`` and the ``rest-xml`` protocols both have XML bodies in their
  responses which are serialized quite similarly (with some specifics for each
  type).
* The ``json`` and the ``rest-json`` protocols both have JSON bodies in their
  responses which are serialized the same way.
* The ``rest-json`` and ``rest-xml`` protocols serialize some metadata in
  the HTTP response's header fields
* The ``ec2`` protocol is basically similar to the ``query`` protocol with a
  specific error response formatting.

The serializer classes in this module correspond directly to the different
protocols. ``#create_serializer`` shows the explicit mapping between the
classes and the protocols.
The base classes are structured as follows:
* The ``ResponseSerializer`` contains all the basic logic for the parsing
  which is shared among all different protocols.
* The ``BaseXMLResponseSerializer`` and the ``JSONResponseSerializer``
  contain the logic for the XML and the JSON serialization respectively.
* The ``BaseRestResponseSerializer`` contains the logic for the REST
  protocol specifics (i.e. specific HTTP header serializations).
* The ``RestXMLResponseSerializer`` and the ``RestJSONResponseSerializer``
  inherit the ReST specific logic from the ``BaseRestResponseSerializer``
  and the XML / JSON body serialization from their second super class.

The services and their protocols are defined by using AWS's Smithy
(a language to define services in a - somewhat - protocol-agnostic
way). The "peculiarities" in this serializer code usually correspond
to certain "traits" in Smithy.

The result of the serialization methods is the HTTP response which can be sent
back to the calling client.
"""
import abc
import base64
import calendar
import json
from datetime import datetime
from email.utils import formatdate
from typing import Optional, Union
from xml.etree import ElementTree as ETree

import six
from boto.utils import ISO8601
from botocore.model import ListShape, MapShape, OperationModel, ServiceModel, Shape, StructureShape
from botocore.serialize import ISO8601_MICRO
from botocore.utils import conditionally_calculate_md5, parse_to_aware_datetime
from moto.core.utils import gen_amzn_requestid_long

from localstack.aws.api import CommonServiceException, HttpResponse, ServiceException


class ResponseSerializer(abc.ABC):
    """
    The response serializer is responsible for the serialization of a service implementation's result to an actual
    HTTP response (which will be sent to the calling client).
    It is the base class of all serializers and therefore contains the basic logic which is used among all of them.
    """

    DEFAULT_ENCODING = "utf-8"

    def serialize_to_response(
        self, response: dict, operation_model: OperationModel
    ) -> HttpResponse:
        """
        Takes a response dict and serializes it to an actual HttpResponse.

        :param response: to serialize
        :param operation_model: specification of the service & operation containing information about the shape of the
                                service's output / response
        :return: HttpResponse which can be sent to the calling client
        """
        serialized_response = self._create_default_response(operation_model)
        shape = operation_model.output_shape
        # The shape can also be none (for empty responses), but it still needs to be serialized (to add some metadata)
        shape_members = shape.members if shape is not None else None
        self._serialize_payload(
            response, serialized_response, shape, shape_members, operation_model
        )
        serialized_response = self._prepare_additional_traits_in_response(
            serialized_response, operation_model
        )
        return serialized_response

    def serialize_error_to_response(
        self, error: ServiceException, operation_model: OperationModel
    ) -> HttpResponse:
        """
        Takes an error instance and serializes it to an actual HttpResponse.
        Therefore this method is used for errors which should be serialized and transmitted to the calling client.

        :param error: to serialize
        :param operation_model: specification of the service & operation containing information about the shape of the
                                service's output / response
        :return: HttpResponse which can be sent to the calling client
        """
        serialized_response = self._create_default_response(operation_model)
        if isinstance(error, CommonServiceException):
            # Not all possible exceptions are contained in the service's specification.
            # Therefore, service implementations can also throw a "CommonServiceException" to raise arbitrary /
            # non-specified exceptions (where the developer needs to define the data which would usually be taken from
            # the specification, like the "Code").
            code = error.code
            sender_fault = error.sender_fault
            status_code = error.status_code
            shape = None
        else:
            # It it's not a CommonServiceException, the exception is being serialized based on the specification

            # The shape name is equal to the class name (since the classes are generated from the shape's name)
            error_shape_name = error.__class__.__name__

            # Lookup the corresponding error shape in the operation model
            shape = next(
                shape for shape in operation_model.error_shapes if shape.name == error_shape_name
            )
            error_spec = shape.metadata.get("error", {})
            status_code = error_spec.get("httpStatusCode")

            # If the code is not explicitly set, it's typically the shape's name
            code = error_spec.get("code", shape.name)

            # The senderFault is only set if the "senderFault" is true
            # (there are no examples which show otherwise)
            sender_fault = error_spec.get("senderFault")

        # Some specifications do not contain the httpStatusCode field.
        # These errors typically have the http status code 400.
        serialized_response["status_code"] = status_code or 400

        self._serialize_error(
            error, code, sender_fault, serialized_response, shape, operation_model
        )
        serialized_response = self._prepare_additional_traits_in_response(
            serialized_response, operation_model
        )
        return serialized_response

    def _serialize_payload(
        self,
        parameters: dict,
        serialized: HttpResponse,
        shape: Optional[Shape],
        shape_members: dict,
        operation_model: OperationModel,
    ) -> None:
        # TODO implement the handling of location traits (where "location" is "header", "headers", or "path")
        # TODO implement the handling of eventstreams (where "streaming" is True)
        raise NotImplementedError

    def _serialize_error(
        self,
        error: ServiceException,
        code: str,
        sender_fault: bool,
        serialized: HttpResponse,
        shape: Shape,
        operation_model: OperationModel,
    ) -> None:
        raise NotImplementedError

    def _create_default_response(self, operation_model: OperationModel) -> HttpResponse:
        """
        Creates a boilerplate default response dict to be used by subclasses as starting points.
        Uses the default HTTP response status code defined in the operation model (if defined).

        :param operation_model: to extract the default HTTP status code
        :return: boilerplate HTTP response
        """
        return HttpResponse(
            headers={}, body=b"", status_code=operation_model.http.get("responseCode", 200)
        )

    # Some extra utility methods subclasses can use.

    def _timestamp_iso8601(self, value: datetime) -> str:
        if value.microsecond > 0:
            timestamp_format = ISO8601_MICRO
        else:
            timestamp_format = ISO8601
        return value.strftime(timestamp_format)

    def _timestamp_unixtimestamp(self, value: datetime) -> int:
        return int(calendar.timegm(value.timetuple()))

    def _timestamp_rfc822(self, value: datetime) -> str:
        if isinstance(value, datetime):
            value = self._timestamp_unixtimestamp(value)
        return formatdate(value, usegmt=True)

    def _convert_timestamp_to_str(
        self, value: Union[int, str, datetime], timestamp_format=None
    ) -> str:
        if timestamp_format is None:
            timestamp_format = self.TIMESTAMP_FORMAT
        timestamp_format = timestamp_format.lower()
        datetime_obj = parse_to_aware_datetime(value)
        converter = getattr(self, "_timestamp_%s" % timestamp_format)
        final_value = converter(datetime_obj)
        return final_value

    def _get_serialized_name(self, shape: Shape, default_name: str) -> str:
        """
        Returns the serialized name for the shape if it exists.
        Otherwise it will return the passed in default_name.
        """
        return shape.serialization.get("name", default_name)

    def _get_base64(self, value: Union[str, bytes]):
        """
        Returns the base64-encoded version of value, handling
        both strings and bytes. The returned value is a string
        via the default encoding.
        """
        if isinstance(value, six.text_type):
            value = value.encode(self.DEFAULT_ENCODING)
        return base64.b64encode(value).strip().decode(self.DEFAULT_ENCODING)

    def _prepare_additional_traits_in_response(
        self, response: HttpResponse, operation_model: OperationModel
    ):
        """Applies additional traits on the raw response for a given model or protocol."""
        if operation_model.http_checksum_required:
            conditionally_calculate_md5(response)
        return response


class BaseXMLResponseSerializer(ResponseSerializer):
    """
    The BaseXMLResponseSerializer performs the basic logic for the XML response serialization.
    It is slightly adapted by the QueryResponseSerializer.
    While the botocore's RestXMLSerializer is quite similar, there are some subtle differences (since botocore's
    implementation handles the serialization of the requests from the client to the service, not the responses from the
    service to the client).
    """

    TIMESTAMP_FORMAT = "iso8601"

    def _serialize_payload(
        self,
        parameters: dict,
        serialized: HttpResponse,
        shape: Optional[Shape],
        shape_members: dict,
        operation_model: OperationModel,
    ) -> None:
        """
        Serializes the given parameters as XML.
        :param parameters: The user input params
        :param serialized: The final serialized response dict
        :param shape: Describes the expected output shape (can be None in case of an "empty" response)
        :param shape_members: The members of the output struct shape
        :param operation_model: The specification of the operation of which the response is serialized here
        :return: None - the given `serialized` dict is modified
        """
        payload_member = shape.serialization.get("payload") if shape is not None else None
        if payload_member is not None and shape_members[payload_member].type_name in [
            "blob",
            "string",
        ]:
            # If it's streaming, then the body is just the value of the payload.
            body_payload = parameters.get(payload_member, b"")
            body_payload = self._encode_payload(body_payload)
            serialized["body"] = body_payload
        elif payload_member is not None:
            # If there's a payload member, we serialized that member to the body.
            body_params = parameters.get(payload_member)
            if body_params is not None:
                serialized["body"] = self._encode_payload(
                    self._serialize_body_params(
                        body_params, shape_members[payload_member], operation_model
                    )
                )
        else:
            # Otherwise we use the "traditional" way of serializing the whole parameters dict recursively.
            serialized["body"] = self._encode_payload(
                self._serialize_body_params(parameters, shape, operation_model)
            )

    def _serialize_error(
        self,
        error: ServiceException,
        code: str,
        sender_fault: bool,
        serialized: HttpResponse,
        shape: Shape,
        operation_model: OperationModel,
    ) -> None:
        # TODO handle error shapes with members
        # Check if we need to add a namespace
        attr = (
            {"xmlns": operation_model.metadata.get("xmlNamespace")}
            if "xmlNamespace" in operation_model.metadata
            else {}
        )
        root = ETree.Element("ErrorResponse", attr)
        error_tag = ETree.SubElement(root, "Error")
        self._add_error_tags(code, error, error_tag, sender_fault)
        request_id = ETree.SubElement(root, "RequestId")
        request_id.text = gen_amzn_requestid_long()
        serialized["body"] = self._encode_payload(
            ETree.tostring(root, encoding=self.DEFAULT_ENCODING)
        )

    def _add_error_tags(
        self, code: str, error: ServiceException, error_tag: ETree.Element, sender_fault: bool
    ) -> None:
        code_tag = ETree.SubElement(error_tag, "Code")
        code_tag.text = code
        message = str(error)
        if len(message) > 0:
            message_tag = ETree.SubElement(error_tag, "Message")
            message_tag.text = message
        if sender_fault:
            # The sender fault is either not set or "Sender"
            fault_tag = ETree.SubElement(error_tag, "Fault")
            fault_tag.text = "Sender"

    def _serialize_body_params(
        self, params: dict, shape: Shape, operation_model: OperationModel
    ) -> str:
        root = self._serialize_body_params_to_xml(params, shape, operation_model)
        self._prepare_additional_traits_in_xml(root)
        return ETree.tostring(root, encoding=self.DEFAULT_ENCODING)

    def _serialize_body_params_to_xml(
        self, params: dict, shape: Shape, operation_model: OperationModel
    ) -> Optional[ETree.Element]:
        if shape is None:
            return
        # The botocore serializer expects `shape.serialization["name"]`, but this isn't always present for responses
        root_name = shape.serialization.get("name", shape.name)
        pseudo_root = ETree.Element("")
        self._serialize(shape, params, pseudo_root, root_name)
        real_root = list(pseudo_root)[0]
        return real_root

    def _encode_payload(self, body: Union[bytes, str]) -> bytes:
        if isinstance(body, six.text_type):
            return body.encode(self.DEFAULT_ENCODING)
        return body

    def _serialize(self, shape: Shape, params: any, xmlnode: ETree.Element, name: str) -> None:
        """This method dynamically invokes the correct `_serialize_type_*` method for each shape type."""
        if shape is None:
            return
        # Some output shapes define a `resultWrapper` in their serialization spec.
        # While the name would imply that the result is _wrapped_, it is actually renamed.
        if shape.serialization.get("resultWrapper"):
            name = shape.serialization.get("resultWrapper")

        method = getattr(self, "_serialize_type_%s" % shape.type_name, self._default_serialize)
        method(xmlnode, params, shape, name)

    def _serialize_type_structure(
        self, xmlnode: ETree.Element, params: dict, shape: StructureShape, name: str
    ) -> None:
        structure_node = ETree.SubElement(xmlnode, name)

        if "xmlNamespace" in shape.serialization:
            namespace_metadata = shape.serialization["xmlNamespace"]
            attribute_name = "xmlns"
            if namespace_metadata.get("prefix"):
                attribute_name += ":%s" % namespace_metadata["prefix"]
            structure_node.attrib[attribute_name] = namespace_metadata["uri"]
        for key, value in params.items():
            member_shape = shape.members[key]
            member_name = member_shape.serialization.get("name", key)
            # We need to special case member shapes that are marked as an xmlAttribute.
            # Rather than serializing into an XML child node, we instead serialize the shape to
            # an XML attribute of the *current* node.
            if value is None:
                # Don't serialize any param whose value is None.
                continue
            if member_shape.serialization.get("xmlAttribute"):
                # xmlAttributes must have a serialization name.
                xml_attribute_name = member_shape.serialization["name"]
                structure_node.attrib[xml_attribute_name] = value
                continue
            self._serialize(member_shape, value, structure_node, member_name)

    def _serialize_type_list(
        self, xmlnode: ETree.Element, params: list, shape: ListShape, name: str
    ) -> None:
        member_shape = shape.member
        if shape.serialization.get("flattened"):
            # If the list is flattened, either take the member's "name" or the name of the usual name for the parent
            # element for the children.
            element_name = self._get_serialized_name(member_shape, name)
            list_node = xmlnode
        else:
            element_name = self._get_serialized_name(member_shape, "member")
            list_node = ETree.SubElement(xmlnode, name)
        for item in params:
            self._serialize(member_shape, item, list_node, element_name)

    def _serialize_type_map(
        self, xmlnode: ETree.Element, params: dict, shape: MapShape, name: str
    ) -> None:
        """
        Given the ``name`` of MyMap, an input of {"key1": "val1", "key2": "val2"}, and the ``flattened: False``
        we serialize this as:
          <MyMap>
            <entry>
              <key>key1</key>
              <value>val1</value>
            </entry>
            <entry>
              <key>key2</key>
              <value>val2</value>
            </entry>
          </MyMap>
        If it is flattened, it is serialized as follows:
          <MyMap>
            <key>key1</key>
            <value>val1</value>
          </MyMap>
          <MyMap>
            <key>key2</key>
            <value>val2</value>
          </MyMap>
        """
        if shape.serialization.get("flattened"):
            entries_node = xmlnode
            entry_node_name = name
        else:
            entries_node = ETree.SubElement(xmlnode, name)
            entry_node_name = "entry"

        for key, value in params.items():
            entry_node = ETree.SubElement(entries_node, entry_node_name)
            key_name = self._get_serialized_name(shape.key, default_name="key")
            val_name = self._get_serialized_name(shape.value, default_name="value")
            self._serialize(shape.key, key, entry_node, key_name)
            self._serialize(shape.value, value, entry_node, val_name)

    def _serialize_type_boolean(self, xmlnode: ETree.Element, params: bool, _, name: str) -> None:
        """
        For scalar types, the 'params' attr is actually just a scalar value representing the data
        we need to serialize as a boolean. It will either be 'true' or 'false'
        """
        node = ETree.SubElement(xmlnode, name)
        if params:
            str_value = "true"
        else:
            str_value = "false"
        node.text = str_value

    def _serialize_type_blob(
        self, xmlnode: ETree.Element, params: Union[str, bytes], _, name: str
    ) -> None:
        node = ETree.SubElement(xmlnode, name)
        node.text = self._get_base64(params)

    def _serialize_type_timestamp(
        self, xmlnode: ETree.Element, params: str, shape: Shape, name: str
    ) -> None:
        node = ETree.SubElement(xmlnode, name)
        node.text = self._convert_timestamp_to_str(
            params, shape.serialization.get("timestampFormat")
        )

    def _default_serialize(self, xmlnode: ETree.Element, params: str, _, name: str) -> None:
        node = ETree.SubElement(xmlnode, name)
        node.text = six.text_type(params)

    def _prepare_additional_traits_in_xml(self, root: Optional[ETree.Element]):
        """
        Prepares the XML root node before being serialized with additional traits (like the Response ID in the Query
        protocol).
        For some protocols (like rest-xml), the root can be None.
        """
        pass


class BaseRestResponseSerializer(ResponseSerializer):
    """
    The BaseRestResponseSerializer performs the basic logic for the ReST response serialization.
    In our case it basically only adds the request metadata to the HTTP header.
    """

    def _prepare_additional_traits_in_response(
        self, response: HttpResponse, operation_model: OperationModel
    ):
        """Adds the request ID to the headers (in contrast to the body - as in the Query protocol)."""
        response = super()._prepare_additional_traits_in_response(response, operation_model)
        response["headers"]["x-amz-request-id"] = gen_amzn_requestid_long()
        return response


class RestXMLResponseSerializer(BaseRestResponseSerializer, BaseXMLResponseSerializer):
    """
    The ``RestXMLResponseSerializer`` is responsible for the serialization of responses from services with the
    ``rest-xml`` protocol.
    It combines the ``BaseRestResponseSerializer`` (for the ReST specific logic) with the ``BaseXMLResponseSerializer``
    (for the XML body response serialization), and adds some minor logic to handle S3 specific peculiarities with the
    error response serialization.
    """

    def _serialize_error(
        self,
        error: ServiceException,
        code: str,
        sender_fault: bool,
        serialized: HttpResponse,
        shape: Shape,
        operation_model: OperationModel,
    ) -> None:
        # It wouldn't be a spec if there wouldn't be any exceptions.
        # S3 errors look differently than other service's errors.
        if operation_model.name == "s3":
            attr = (
                {"xmlns": operation_model.metadata.get("xmlNamespace")}
                if "xmlNamespace" in operation_model.metadata
                else None
            )
            root = ETree.Element("Error", attr)
            self._add_error_tags(code, error, root, sender_fault)
            request_id = ETree.SubElement(root, "RequestId")
            request_id.text = gen_amzn_requestid_long()
            serialized["body"] = self._encode_payload(
                ETree.tostring(root, encoding=self.DEFAULT_ENCODING)
            )
        else:
            super()._serialize_error(error, code, sender_fault, serialized, shape, operation_model)


class QueryResponseSerializer(BaseXMLResponseSerializer):
    """
    The ``QueryResponseSerializer`` is responsible for the serialization of responses from services which use the
    ``query`` protocol. The responses of these services also use XML, but with a few subtle differences to the
    ``rest-xml`` protocol.
    """

    def _serialize_body_params_to_xml(
        self, params: dict, shape: Shape, operation_model: OperationModel
    ) -> ETree.Element:
        # The Query protocol responses have a root element which is not contained in the specification file.
        # Therefore we first call the super function to perform the normal XML serialization, and afterwards wrap the
        # result in a root element based on the operation name.
        node = super()._serialize_body_params_to_xml(params, shape, operation_model)

        # Check if we need to add a namespace
        attr = (
            {"xmlns": operation_model.metadata.get("xmlNamespace")}
            if "xmlNamespace" in operation_model.metadata
            else None
        )

        # Create the root element and add the result of the XML serializer as a child node
        root = ETree.Element(f"{operation_model.name}Response", attr)
        if node is not None:
            root.append(node)
        return root

    def _prepare_additional_traits_in_xml(self, root: Optional[ETree.Element]):
        # Add the response metadata here (it's not defined in the specs)
        # For the ec2 and the query protocol, the root cannot be None at this time.
        response_metadata = ETree.SubElement(root, "ResponseMetadata")
        request_id = ETree.SubElement(response_metadata, "RequestId")
        request_id.text = gen_amzn_requestid_long()


class EC2ResponseSerializer(QueryResponseSerializer):
    """
    The ``EC2ResponseSerializer`` is responsible for the serialization of responses from services which use the
    ``ec2`` protocol (basically the EC2 service). This protocol is basically equal to the ``query`` protocol with only
    a few subtle differences.
    """

    def _serialize_error(
        self,
        error: ServiceException,
        code: str,
        sender_fault: bool,
        serialized: HttpResponse,
        shape: Shape,
        operation_model: OperationModel,
    ) -> None:
        # EC2 errors look like:
        # <Response>
        #   <Errors>
        #     <Error>
        #       <Code>InvalidInstanceID.Malformed</Code>
        #       <Message>Invalid id: "1343124"</Message>
        #     </Error>
        #   </Errors>
        #   <RequestID>12345</RequestID>
        # </Response>
        # This is different from QueryParser in that it's RequestID, not RequestId
        # and that the Error tag is in an enclosing Errors tag.
        attr = (
            {"xmlns": operation_model.metadata.get("xmlNamespace")}
            if "xmlNamespace" in operation_model.metadata
            else None
        )
        root = ETree.Element("Errors", attr)
        error_tag = ETree.SubElement(root, "Error")
        self._add_error_tags(code, error, error_tag, sender_fault)
        request_id = ETree.SubElement(root, "RequestID")
        request_id.text = gen_amzn_requestid_long()
        serialized["body"] = self._encode_payload(
            ETree.tostring(root, encoding=self.DEFAULT_ENCODING)
        )

    def _prepare_additional_traits_in_xml(self, root: Optional[ETree.Element]):
        # The EC2 protocol does not use the root output shape, therefore we need to remove the hierarchy level
        # below the root level
        output_node = root[0]
        for child in output_node:
            root.append(child)
        root.remove(output_node)

        # Add the requestId here (it's not defined in the specs)
        # For the ec2 and the query protocol, the root cannot be None at this time.
        request_id = ETree.SubElement(root, "requestId")
        request_id.text = gen_amzn_requestid_long()


class JSONResponseSerializer(ResponseSerializer):
    """
    The ``JSONResponseSerializer`` is responsible for the serialization of responses from services with the ``json``
    protocol. It implements the JSON response body serialization, which is also used by the
    ``RestJSONResponseSerializer``.
    """

    TIMESTAMP_FORMAT = "unixtimestamp"

    def _serialize_error(
        self,
        error: ServiceException,
        code: str,
        sender_fault: bool,
        serialized: HttpResponse,
        shape: Shape,
        operation_model: OperationModel,
    ) -> None:
        # TODO handle error shapes with members
        body = {"__type": code, "message": str(error)}
        serialized["body"] = json.dumps(body).encode(self.DEFAULT_ENCODING)

    def _serialize_payload(
        self,
        parameters: dict,
        serialized: HttpResponse,
        shape: Optional[Shape],
        shape_members: dict,
        operation_model: OperationModel,
    ) -> None:
        json_version = operation_model.metadata.get("jsonVersion")
        if json_version is not None:
            serialized["headers"] = {
                "Content-Type": "application/x-amz-json-%s" % json_version,
            }
        body = {}
        if shape is not None:
            self._serialize(body, parameters, shape)
        serialized["body"] = json.dumps(body).encode(self.DEFAULT_ENCODING)

    def _serialize(self, body: dict, value: any, shape, key: Optional[str] = None):
        """This method dynamically invokes the correct `_serialize_type_*` method for each shape type."""
        method = getattr(self, "_serialize_type_%s" % shape.type_name, self._default_serialize)
        method(body, value, shape, key)

    def _serialize_type_structure(self, body: dict, value: dict, shape: StructureShape, key: str):
        if shape.is_document_type:
            body[key] = value
        else:
            if key is not None:
                # If a key is provided, this is a result of a recursive
                # call so we need to add a new child dict as the value
                # of the passed in serialized dict.  We'll then add
                # all the structure members as key/vals in the new serialized
                # dictionary we just created.
                new_serialized = {}
                body[key] = new_serialized
                body = new_serialized
            members = shape.members
            for member_key, member_value in value.items():
                member_shape = members[member_key]
                if "name" in member_shape.serialization:
                    member_key = member_shape.serialization["name"]
                self._serialize(body, member_value, member_shape, member_key)

    def _serialize_type_map(self, body: dict, value: dict, shape: MapShape, key: str):
        map_obj = {}
        body[key] = map_obj
        for sub_key, sub_value in value.items():
            self._serialize(map_obj, sub_value, shape.value, sub_key)

    def _serialize_type_list(self, body: dict, value: list, shape: ListShape, key: str):
        list_obj = []
        body[key] = list_obj
        for list_item in value:
            wrapper = {}
            # The JSON list serialization is the only case where we aren't
            # setting a key on a dict.  We handle this by using
            # a __current__ key on a wrapper dict to serialize each
            # list item before appending it to the serialized list.
            self._serialize(wrapper, list_item, shape.member, "__current__")
            list_obj.append(wrapper["__current__"])

    def _default_serialize(self, body: dict, value: any, _, key: str):
        body[key] = value

    def _serialize_type_timestamp(self, body: dict, value: any, shape: Shape, key: str):
        body[key] = self._convert_timestamp_to_str(
            value, shape.serialization.get("timestampFormat")
        )

    def _serialize_type_blob(self, body: dict, value: Union[str, bytes], _, key: str):
        body[key] = self._get_base64(value)

    def _prepare_additional_traits_in_response(
        self, response: HttpResponse, operation_model: OperationModel
    ):
        response["headers"]["x-amzn-requestid"] = gen_amzn_requestid_long()
        response = super()._prepare_additional_traits_in_response(response, operation_model)
        return response


class RestJSONResponseSerializer(BaseRestResponseSerializer, JSONResponseSerializer):
    """
    The ``RestJSONResponseSerializer`` is responsible for the serialization of responses from services with the
    ``rest-json`` protocol.
    It combines the ``BaseRestResponseSerializer`` (for the ReST specific logic) with the ``JSONResponseSerializer``
    (for the JSOn body response serialization).
    """

    pass


def create_serializer(service: ServiceModel) -> ResponseSerializer:
    """
    Creates the right serializer for the given service model.
    :param service: to create the serializer for.
    :return: ResponseSerializer which can handle the protocol of the service.
    """
    serializers = {
        "query": QueryResponseSerializer,
        "json": JSONResponseSerializer,
        "rest-json": RestJSONResponseSerializer,
        "rest-xml": RestXMLResponseSerializer,
        "ec2": EC2ResponseSerializer,
    }
    return serializers[service.protocol]()
