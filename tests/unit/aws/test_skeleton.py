from typing import Dict, List, TypedDict

from botocore.parsers import create_parser

from localstack.aws.api import RequestContext, ServiceException, ServiceRequest, handler

__all__ = [
    "AWSAccountIdList",
    "ActionNameList",
    "AddPermissionRequest",
    "AttributeNameList",
    "BatchEntryIdsNotDistinct",
    "BatchRequestTooLong",
    "BatchResultErrorEntry",
    "BatchResultErrorEntryList",
    "Binary",
    "BinaryList",
    "Boolean",
    "BoxedInteger",
    "ChangeMessageVisibilityBatchRequest",
    "ChangeMessageVisibilityBatchRequestEntry",
    "ChangeMessageVisibilityBatchRequestEntryList",
    "ChangeMessageVisibilityBatchResult",
    "ChangeMessageVisibilityBatchResultEntry",
    "ChangeMessageVisibilityBatchResultEntryList",
    "ChangeMessageVisibilityRequest",
    "CreateQueueRequest",
    "CreateQueueResult",
    "DeleteMessageBatchRequest",
    "DeleteMessageBatchRequestEntry",
    "DeleteMessageBatchRequestEntryList",
    "DeleteMessageBatchResult",
    "DeleteMessageBatchResultEntry",
    "DeleteMessageBatchResultEntryList",
    "DeleteMessageRequest",
    "DeleteQueueRequest",
    "EmptyBatchRequest",
    "GetQueueAttributesRequest",
    "GetQueueAttributesResult",
    "GetQueueUrlRequest",
    "GetQueueUrlResult",
    "Integer",
    "InvalidAttributeName",
    "InvalidBatchEntryId",
    "InvalidIdFormat",
    "InvalidMessageContents",
    "ListDeadLetterSourceQueuesRequest",
    "ListDeadLetterSourceQueuesResult",
    "ListQueueTagsRequest",
    "ListQueueTagsResult",
    "ListQueuesRequest",
    "ListQueuesResult",
    "Message",
    "MessageAttributeName",
    "MessageAttributeNameList",
    "MessageAttributeValue",
    "MessageBodyAttributeMap",
    "MessageBodySystemAttributeMap",
    "MessageList",
    "MessageNotInflight",
    "MessageSystemAttributeMap",
    "MessageSystemAttributeName",
    "MessageSystemAttributeNameForSends",
    "MessageSystemAttributeValue",
    "OverLimit",
    "PurgeQueueInProgress",
    "PurgeQueueRequest",
    "QueueAttributeMap",
    "QueueAttributeName",
    "QueueDeletedRecently",
    "QueueDoesNotExist",
    "QueueNameExists",
    "QueueUrlList",
    "ReceiptHandleIsInvalid",
    "ReceiveMessageRequest",
    "ReceiveMessageResult",
    "RemovePermissionRequest",
    "SendMessageBatchRequest",
    "SendMessageBatchRequestEntry",
    "SendMessageBatchRequestEntryList",
    "SendMessageBatchResult",
    "SendMessageBatchResultEntry",
    "SendMessageBatchResultEntryList",
    "SendMessageRequest",
    "SendMessageResult",
    "SetQueueAttributesRequest",
    "String",
    "StringList",
    "TagKey",
    "TagKeyList",
    "TagMap",
    "TagQueueRequest",
    "TagValue",
    "Token",
    "TooManyEntriesInBatchRequest",
    "UnsupportedOperation",
    "UntagQueueRequest",
]

from localstack.aws.skeleton import Skeleton
from localstack.aws.spec import load_service

Boolean = bool
BoxedInteger = int
Integer = int
MessageAttributeName = str
String = str
TagKey = str
TagValue = str
Token = str


class MessageSystemAttributeName(str):
    SenderId = "SenderId"
    SentTimestamp = "SentTimestamp"
    ApproximateReceiveCount = "ApproximateReceiveCount"
    ApproximateFirstReceiveTimestamp = "ApproximateFirstReceiveTimestamp"
    SequenceNumber = "SequenceNumber"
    MessageDeduplicationId = "MessageDeduplicationId"
    MessageGroupId = "MessageGroupId"
    AWSTraceHeader = "AWSTraceHeader"


class MessageSystemAttributeNameForSends(str):
    AWSTraceHeader = "AWSTraceHeader"


class QueueAttributeName(str):
    All = "All"
    Policy = "Policy"
    VisibilityTimeout = "VisibilityTimeout"
    MaximumMessageSize = "MaximumMessageSize"
    MessageRetentionPeriod = "MessageRetentionPeriod"
    ApproximateNumberOfMessages = "ApproximateNumberOfMessages"
    ApproximateNumberOfMessagesNotVisible = "ApproximateNumberOfMessagesNotVisible"
    CreatedTimestamp = "CreatedTimestamp"
    LastModifiedTimestamp = "LastModifiedTimestamp"
    QueueArn = "QueueArn"
    ApproximateNumberOfMessagesDelayed = "ApproximateNumberOfMessagesDelayed"
    DelaySeconds = "DelaySeconds"
    ReceiveMessageWaitTimeSeconds = "ReceiveMessageWaitTimeSeconds"
    RedrivePolicy = "RedrivePolicy"
    FifoQueue = "FifoQueue"
    ContentBasedDeduplication = "ContentBasedDeduplication"
    KmsMasterKeyId = "KmsMasterKeyId"
    KmsDataKeyReusePeriodSeconds = "KmsDataKeyReusePeriodSeconds"
    DeduplicationScope = "DeduplicationScope"
    FifoThroughputLimit = "FifoThroughputLimit"
    RedriveAllowPolicy = "RedriveAllowPolicy"


class BatchEntryIdsNotDistinct(ServiceException):
    pass


class BatchRequestTooLong(ServiceException):
    pass


class EmptyBatchRequest(ServiceException):
    pass


class InvalidAttributeName(ServiceException):
    pass


class InvalidBatchEntryId(ServiceException):
    pass


class InvalidIdFormat(ServiceException):
    pass


class InvalidMessageContents(ServiceException):
    pass


class MessageNotInflight(ServiceException):
    pass


class OverLimit(ServiceException):
    pass


class PurgeQueueInProgress(ServiceException):
    pass


class QueueDeletedRecently(ServiceException):
    pass


class QueueDoesNotExist(ServiceException):
    pass


class QueueNameExists(ServiceException):
    pass


class ReceiptHandleIsInvalid(ServiceException):
    pass


class TooManyEntriesInBatchRequest(ServiceException):
    pass


class UnsupportedOperation(ServiceException):
    pass


AWSAccountIdList = List[String]

ActionNameList = List[String]


class AddPermissionRequest(ServiceRequest):
    QueueUrl: String
    Label: String
    AWSAccountIds: AWSAccountIdList
    Actions: ActionNameList


AttributeNameList = List[QueueAttributeName]


class BatchResultErrorEntry(TypedDict):
    Id: String
    SenderFault: Boolean
    Code: String
    Message: String


BatchResultErrorEntryList = List[BatchResultErrorEntry]

Binary = bytes
BinaryList = List[Binary]


class ChangeMessageVisibilityBatchRequestEntry(TypedDict):
    Id: String
    ReceiptHandle: String
    VisibilityTimeout: Integer


ChangeMessageVisibilityBatchRequestEntryList = List[ChangeMessageVisibilityBatchRequestEntry]


class ChangeMessageVisibilityBatchRequest(ServiceRequest):
    QueueUrl: String
    Entries: ChangeMessageVisibilityBatchRequestEntryList


class ChangeMessageVisibilityBatchResultEntry(TypedDict):
    Id: String


ChangeMessageVisibilityBatchResultEntryList = List[ChangeMessageVisibilityBatchResultEntry]


class ChangeMessageVisibilityBatchResult(TypedDict):
    Successful: ChangeMessageVisibilityBatchResultEntryList
    Failed: BatchResultErrorEntryList


class ChangeMessageVisibilityRequest(ServiceRequest):
    QueueUrl: String
    ReceiptHandle: String
    VisibilityTimeout: Integer


TagMap = Dict[TagKey, TagValue]
QueueAttributeMap = Dict[QueueAttributeName, String]


class CreateQueueRequest(ServiceRequest):
    QueueName: String
    Attributes: QueueAttributeMap
    tags: TagMap


class CreateQueueResult(TypedDict):
    QueueUrl: String


class DeleteMessageBatchRequestEntry(TypedDict):
    Id: String
    ReceiptHandle: String


DeleteMessageBatchRequestEntryList = List[DeleteMessageBatchRequestEntry]


class DeleteMessageBatchRequest(ServiceRequest):
    QueueUrl: String
    Entries: DeleteMessageBatchRequestEntryList


class DeleteMessageBatchResultEntry(TypedDict):
    Id: String


DeleteMessageBatchResultEntryList = List[DeleteMessageBatchResultEntry]


class DeleteMessageBatchResult(TypedDict):
    Successful: DeleteMessageBatchResultEntryList
    Failed: BatchResultErrorEntryList


class DeleteMessageRequest(ServiceRequest):
    QueueUrl: String
    ReceiptHandle: String


class DeleteQueueRequest(ServiceRequest):
    QueueUrl: String


class GetQueueAttributesRequest(ServiceRequest):
    QueueUrl: String
    AttributeNames: AttributeNameList


class GetQueueAttributesResult(TypedDict):
    Attributes: QueueAttributeMap


class GetQueueUrlRequest(ServiceRequest):
    QueueName: String
    QueueOwnerAWSAccountId: String


class GetQueueUrlResult(TypedDict):
    QueueUrl: String


class ListDeadLetterSourceQueuesRequest(ServiceRequest):
    QueueUrl: String
    NextToken: Token
    MaxResults: BoxedInteger


QueueUrlList = List[String]


class ListDeadLetterSourceQueuesResult(TypedDict):
    queueUrls: QueueUrlList
    NextToken: Token


class ListQueueTagsRequest(ServiceRequest):
    QueueUrl: String


class ListQueueTagsResult(TypedDict):
    Tags: TagMap


class ListQueuesRequest(ServiceRequest):
    QueueNamePrefix: String
    NextToken: Token
    MaxResults: BoxedInteger


class ListQueuesResult(TypedDict):
    QueueUrls: QueueUrlList
    NextToken: Token


StringList = List[String]


class MessageAttributeValue(TypedDict):
    StringValue: String
    BinaryValue: Binary
    StringListValues: StringList
    BinaryListValues: BinaryList
    DataType: String


MessageBodyAttributeMap = Dict[String, MessageAttributeValue]
MessageSystemAttributeMap = Dict[MessageSystemAttributeName, String]


class Message(TypedDict):
    MessageId: String
    ReceiptHandle: String
    MD5OfBody: String
    Body: String
    Attributes: MessageSystemAttributeMap
    MD5OfMessageAttributes: String
    MessageAttributes: MessageBodyAttributeMap


MessageAttributeNameList = List[MessageAttributeName]


class MessageSystemAttributeValue(TypedDict):
    StringValue: String
    BinaryValue: Binary
    StringListValues: StringList
    BinaryListValues: BinaryList
    DataType: String


MessageBodySystemAttributeMap = Dict[
    MessageSystemAttributeNameForSends, MessageSystemAttributeValue
]
MessageList = List[Message]


class PurgeQueueRequest(ServiceRequest):
    QueueUrl: String


class ReceiveMessageRequest(ServiceRequest):
    QueueUrl: String
    AttributeNames: AttributeNameList
    MessageAttributeNames: MessageAttributeNameList
    MaxNumberOfMessages: Integer
    VisibilityTimeout: Integer
    WaitTimeSeconds: Integer
    ReceiveRequestAttemptId: String


class ReceiveMessageResult(TypedDict):
    Messages: MessageList


class RemovePermissionRequest(ServiceRequest):
    QueueUrl: String
    Label: String


class SendMessageBatchRequestEntry(TypedDict):
    Id: String
    MessageBody: String
    DelaySeconds: Integer
    MessageAttributes: MessageBodyAttributeMap
    MessageSystemAttributes: MessageBodySystemAttributeMap
    MessageDeduplicationId: String
    MessageGroupId: String


SendMessageBatchRequestEntryList = List[SendMessageBatchRequestEntry]


class SendMessageBatchRequest(ServiceRequest):
    QueueUrl: String
    Entries: SendMessageBatchRequestEntryList


class SendMessageBatchResultEntry(TypedDict):
    Id: String
    MessageId: String
    MD5OfMessageBody: String
    MD5OfMessageAttributes: String
    MD5OfMessageSystemAttributes: String
    SequenceNumber: String


SendMessageBatchResultEntryList = List[SendMessageBatchResultEntry]


class SendMessageBatchResult(TypedDict):
    Successful: SendMessageBatchResultEntryList
    Failed: BatchResultErrorEntryList


class SendMessageRequest(ServiceRequest):
    QueueUrl: String
    MessageBody: String
    DelaySeconds: Integer
    MessageAttributes: MessageBodyAttributeMap
    MessageSystemAttributes: MessageBodySystemAttributeMap
    MessageDeduplicationId: String
    MessageGroupId: String


class SendMessageResult(TypedDict):
    MD5OfMessageBody: String
    MD5OfMessageAttributes: String
    MD5OfMessageSystemAttributes: String
    MessageId: String
    SequenceNumber: String


class SetQueueAttributesRequest(ServiceRequest):
    QueueUrl: String
    Attributes: QueueAttributeMap


TagKeyList = List[TagKey]


class TagQueueRequest(ServiceRequest):
    QueueUrl: String
    Tags: TagMap


class UntagQueueRequest(ServiceRequest):
    QueueUrl: String
    TagKeys: TagKeyList


class SqsApi:

    service = "sqs"
    version = "2012-11-05"

    @handler("AddPermission")
    def add_permission(
        self,
        context: RequestContext,
        queue_url: String,
        label: String,
        aws_account_ids: AWSAccountIdList,
        actions: ActionNameList,
    ) -> None:
        raise NotImplementedError

    @handler("ChangeMessageVisibility")
    def change_message_visibility(
        self,
        context: RequestContext,
        queue_url: String,
        receipt_handle: String,
        visibility_timeout: Integer,
    ) -> None:
        raise NotImplementedError

    @handler("ChangeMessageVisibilityBatch")
    def change_message_visibility_batch(
        self,
        context: RequestContext,
        queue_url: String,
        entries: ChangeMessageVisibilityBatchRequestEntryList,
    ) -> ChangeMessageVisibilityBatchResult:
        raise NotImplementedError

    @handler("CreateQueue")
    def create_queue(
        self,
        context: RequestContext,
        queue_name: String,
        attributes: QueueAttributeMap = None,
        tags: TagMap = None,
    ) -> CreateQueueResult:
        raise NotImplementedError

    @handler("DeleteMessage")
    def delete_message(
        self, context: RequestContext, queue_url: String, receipt_handle: String
    ) -> None:
        raise NotImplementedError

    @handler("DeleteMessageBatch")
    def delete_message_batch(
        self,
        context: RequestContext,
        queue_url: String,
        entries: DeleteMessageBatchRequestEntryList,
    ) -> DeleteMessageBatchResult:
        raise NotImplementedError

    @handler("DeleteQueue")
    def delete_queue(self, context: RequestContext, queue_url: String) -> None:
        raise NotImplementedError

    @handler("GetQueueAttributes")
    def get_queue_attributes(
        self,
        context: RequestContext,
        queue_url: String,
        attribute_names: AttributeNameList = None,
    ) -> GetQueueAttributesResult:
        raise NotImplementedError

    @handler("GetQueueUrl")
    def get_queue_url(
        self,
        context: RequestContext,
        queue_name: String,
        queue_owner_aws_account_id: String = None,
    ) -> GetQueueUrlResult:
        raise NotImplementedError

    @handler("ListDeadLetterSourceQueues")
    def list_dead_letter_source_queues(
        self,
        context: RequestContext,
        queue_url: String,
        next_token: Token = None,
        max_results: BoxedInteger = None,
    ) -> ListDeadLetterSourceQueuesResult:
        raise NotImplementedError

    @handler("ListQueueTags")
    def list_queue_tags(self, context: RequestContext, queue_url: String) -> ListQueueTagsResult:
        raise NotImplementedError

    @handler("ListQueues")
    def list_queues(
        self,
        context: RequestContext,
        queue_name_prefix: String = None,
        next_token: Token = None,
        max_results: BoxedInteger = None,
    ) -> ListQueuesResult:
        raise NotImplementedError

    @handler("PurgeQueue")
    def purge_queue(self, context: RequestContext, queue_url: String) -> None:
        raise NotImplementedError

    @handler("ReceiveMessage")
    def receive_message(
        self,
        context: RequestContext,
        queue_url: String,
        attribute_names: AttributeNameList = None,
        message_attribute_names: MessageAttributeNameList = None,
        max_number_of_messages: Integer = None,
        visibility_timeout: Integer = None,
        wait_time_seconds: Integer = None,
        receive_request_attempt_id: String = None,
    ) -> ReceiveMessageResult:
        raise NotImplementedError

    @handler("RemovePermission")
    def remove_permission(self, context: RequestContext, queue_url: String, label: String) -> None:
        raise NotImplementedError

    @handler("SendMessage")
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
        print(f"context: {context}")
        print(f"queue_url: {queue_url}")
        print(f"message_body: {message_body}")
        print(f"delay_seconds: {delay_seconds}")
        print(f"message_attributes: {message_attributes}")
        print(f"message_system_attributes: {message_system_attributes}")
        print(f"message_deduplication_id: {message_deduplication_id}")
        print(f"message_group_id: {message_group_id}")

        return {
            "MD5OfMessageBody": "String",
            "MD5OfMessageAttributes": "String",
            "MD5OfMessageSystemAttributes": "String",
            "MessageId": "String",
            "SequenceNumber": "String",
        }

    @handler("SendMessageBatch")
    def send_message_batch(
        self,
        context: RequestContext,
        queue_url: String,
        entries: SendMessageBatchRequestEntryList,
    ) -> SendMessageBatchResult:
        raise NotImplementedError

    @handler("SetQueueAttributes")
    def set_queue_attributes(
        self, context: RequestContext, queue_url: String, attributes: QueueAttributeMap
    ) -> None:
        print(f"RequestContext: {context}")
        print(f"queue_url: {queue_url}")
        print(f"Attributes: {attributes}")

    @handler("TagQueue")
    def tag_queue(self, context: RequestContext, queue_url: String, tags: TagMap) -> None:
        raise NotImplementedError

    @handler("UntagQueue")
    def untag_queue(self, context: RequestContext, queue_url: String, tag_keys: TagKeyList) -> None:
        raise NotImplementedError


def test_skeleton_e2e_sqs_set_queue_attributes():
    sqs_service = load_service("sqs")
    skeleton = Skeleton(sqs_service, SqsApi)
    context = RequestContext()
    context.account = "test"
    context.region = "us-west-1"
    context.service = sqs_service
    context.request = {
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
    result = skeleton.invoke(context)

    # Use the parser from botocore to parse the serialized response
    response_parser = create_parser(sqs_service.protocol)
    parsed_response = response_parser.parse(
        result, sqs_service.operation_model("SetQueueAttributes").output_shape
    )
    # TODO assert


def test_skeleton_e2e_sqs_send_message():
    sqs_service = load_service("sqs")
    skeleton = Skeleton(sqs_service, SqsApi)
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
    assert parsed_response == {
        "MD5OfMessageBody": "String",
        "MD5OfMessageAttributes": "String",
        "MD5OfMessageSystemAttributes": "String",
        "MessageId": "String",
        "SequenceNumber": "String",
        "ResponseMetadata": {"HTTPHeaders": {}, "HTTPStatusCode": 200},
    }
