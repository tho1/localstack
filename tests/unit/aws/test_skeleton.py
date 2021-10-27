import math
import threading
import time
from typing import Dict, List, TypedDict

from botocore.parsers import create_parser

from localstack.aws.api import RequestContext, ServiceException, handler
from localstack.aws.api.sqs import (
    ActionNameList,
    AWSAccountIdList,
    Integer,
    MessageBodyAttributeMap,
    MessageBodySystemAttributeMap,
    SendMessageResult,
    SqsApi,
    String,
)
from localstack.aws.skeleton import Skeleton
from localstack.aws.spec import load_service


class TestSqsApi(SqsApi):
    """Dummy implementation to test the skeleton with a primitive implementation of an API."""

    def send_message(
        self,
        context: RequestContext,
        queue_url: String,
        message_body: String,
        delay_seconds: Integer = None,
        message_attributes: MessageBodyAttributeMap = None,
        message_system_attributes: MessageBodySystemAttributeMap = None,
        message_deduplication_id: String = None,
        message_group_id: String = None,
    ) -> SendMessageResult:
        return {
            "MD5OfMessageBody": "String",
            "MD5OfMessageAttributes": "String",
            "MD5OfMessageSystemAttributes": "String",
            "MessageId": "String",
            "SequenceNumber": "String",
        }


def test_skeleton_e2e_sqs_send_message():
    sqs_service = load_service("sqs")
    skeleton = Skeleton(sqs_service, TestSqsApi())
    context = RequestContext()
    context.account = "test"
    context.region = "us-west-1"
    context.service = sqs_service
    context.request = {
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
    result = skeleton.invoke(context)

    # Use the parser from botocore to parse the serialized response
    response_parser = create_parser(sqs_service.protocol)
    parsed_response = response_parser.parse(
        result, sqs_service.operation_model("SendMessage").output_shape
    )

    # Test the ResponseMetadata and delete it afterwards
    assert "ResponseMetadata" in parsed_response
    assert "RequestId" in parsed_response["ResponseMetadata"]
    assert len(parsed_response["ResponseMetadata"]["RequestId"]) == 52
    assert "HTTPStatusCode" in parsed_response["ResponseMetadata"]
    assert parsed_response["ResponseMetadata"]["HTTPStatusCode"] == 200
    del parsed_response["ResponseMetadata"]

    # Compare the (remaining) actual payload
    assert parsed_response == {
        "MD5OfMessageBody": "String",
        "MD5OfMessageAttributes": "String",
        "MD5OfMessageSystemAttributes": "String",
        "MessageId": "String",
        "SequenceNumber": "String",
    }


def test_skeleton_e2e_sqs_send_message_not_implemented():
    sqs_service = load_service("sqs")
    skeleton = Skeleton(sqs_service, TestSqsApi())
    context = RequestContext()
    context.account = "test"
    context.region = "us-west-1"
    context.service = sqs_service
    context.request = {
        "method": "POST",
        "path": "/",
        "body": "Action=DeleteMessageBatch&Version=2012-11-05&QueueUrl=http%3A%2F%2Flocalhost%3A4566%2F000000000000%2Ftf-acc-test-queue&DeleteMessageBatchRequestEntry.1.Id=bar&DeleteMessageBatchRequestEntry.1.ReceiptHandle=foo&DeleteMessageBatchRequestEntry.2.Id=bar&DeleteMessageBatchRequestEntry.2.ReceiptHandle=foo",
        "headers": {
            "Remote-Addr": "127.0.0.1",
            "Host": "localhost:4566",
            "Accept-Encoding": "identity",
            "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
            "User-Agent": "Boto3/1.18.36 Python/3.8.10 Linux/5.4.0-88-generic Botocore/1.21.36",
            "X-Amz-Date": "20211009T202120Z",
            "Authorization": "AWS4-HMAC-SHA256 Credential=test/20211009/us-east-1/sqs/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=f01ac21fb20d97a38bd72f40c3494543230668ba41fe4fc490fc3e59c6437315",
            "Content-Length": "300",
            "x-localstack-request-url": "http://localhost:4566/",
            "X-Forwarded-For": "127.0.0.1, localhost:4566",
        },
    }
    result = skeleton.invoke(context)

    # Use the parser from botocore to parse the serialized response
    response_parser = create_parser(sqs_service.protocol)
    parsed_response = response_parser.parse(
        result, sqs_service.operation_model("SendMessage").output_shape
    )

    # Test the ResponseMetadata
    assert "ResponseMetadata" in parsed_response
    assert "RequestId" in parsed_response["ResponseMetadata"]
    assert len(parsed_response["ResponseMetadata"]["RequestId"]) == 52
    assert "HTTPStatusCode" in parsed_response["ResponseMetadata"]
    assert parsed_response["ResponseMetadata"]["HTTPStatusCode"] == 501

    # Compare the (remaining) actual eror payload
    assert "Error" in parsed_response
    assert parsed_response["Error"] == {
        "Code": "InternalFailure",
        "Message": "API action 'DeleteMessageBatch' for service 'sqs' not yet implemented",
    }
