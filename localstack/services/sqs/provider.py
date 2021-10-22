import logging
import random
import re
import string
import threading
from queue import Empty
from queue import Queue as FifoQueue
from typing import Dict, List, NamedTuple

from localstack.aws.api import CommonServiceException, RequestContext, handler
from localstack.aws.api.sqs import (
    AttributeNameList,
    BoxedInteger,
    CreateQueueResult,
    GetQueueAttributesResult,
    GetQueueUrlResult,
    Integer,
    InvalidAttributeName,
    ListQueuesResult,
    Message,
    MessageAttributeNameList,
    MessageBodyAttributeMap,
    MessageBodySystemAttributeMap,
    QueueAttributeMap,
    QueueAttributeName,
    QueueDoesNotExist,
    QueueNameExists,
    ReceiveMessageResult,
    SendMessageResult,
    SqsApi,
    String,
    TagMap,
    Token,
)
from localstack.config import get_edge_url
from localstack.utils.common import long_uid, md5, now

LOG = logging.getLogger(__name__)


def generate_message_id():
    return long_uid()


def generate_receipt_handle():
    # http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/ImportantIdentifiers.html#ImportantIdentifiers-receipt-handles
    return "".join(random.choices(string.ascii_letters + string.digits, k=172)) + "="


class InvalidParameterValues(CommonServiceException):
    def __init__(self, message):
        super().__init__("InvalidParameterValues", message, 400, True)


class NonExistentQueue(CommonServiceException):
    def __init__(self):
        # TODO: not sure if this is really how AWS behaves
        super().__init__(
            "AWS.SimpleQueueService.NonExistentQueue",
            "The specified queue does not exist for this wsdl version.",
            status_code=400,
        )


def assert_queue_name(queue_name: str):
    if not re.match(r"^[a-zA-Z0-9_-]{1,80}$", queue_name):
        raise InvalidParameterValues(
            "Can only include alphanumeric characters, hyphens, or underscores. 1 to 80 in length"
        )


class QueueKey(NamedTuple):
    region: str
    account_id: str
    name: str


class SqsQueue:
    key: QueueKey

    attributes: QueueAttributeMap
    tags: TagMap
    message: List[Message]

    def __init__(self, key: QueueKey, attributes=None, tags=None) -> None:
        super().__init__()
        self.key = key
        self.tags = tags or dict()
        self.messages = FifoQueue()

        self.attributes = self.default_attributes()
        if attributes:
            self.attributes.update(attributes)

    def default_attributes(self) -> QueueAttributeMap:
        return {
            QueueAttributeName.QueueArn: self.arn,
            QueueAttributeName.ApproximateNumberOfMessages: "0",
            QueueAttributeName.ApproximateNumberOfMessagesNotVisible: "0",
            QueueAttributeName.ApproximateNumberOfMessagesDelayed: "0",
            QueueAttributeName.CreatedTimestamp: str(now()),
            QueueAttributeName.LastModifiedTimestamp: str(now()),
            QueueAttributeName.VisibilityTimeout: "30",
            QueueAttributeName.MaximumMessageSize: "262144",
            QueueAttributeName.MessageRetentionPeriod: "345600",
            QueueAttributeName.DelaySeconds: "0",
            QueueAttributeName.ReceiveMessageWaitTimeSeconds: "0",
        }

    @property
    def name(self):
        return self.key.name

    @property
    def arn(self) -> str:
        return f"arn:aws:sqs:{self.key.region}:{self.key.account_id}:{self.key.name}"

    @property
    def url(self) -> str:
        return "{host}/{account_id}/{name}".format(
            host=get_edge_url(),  # FIXME region
            account_id=self.key.account_id,
            name=self.key.name,
        )


class SqsProvider(SqsApi):
    # TODO: thread safety

    queues: Dict[QueueKey, SqsQueue]
    queue_url_index: Dict[str, SqsQueue]

    def __init__(self) -> None:
        super().__init__()
        self.queues = dict()
        self.queue_url_index = dict()

    def _add_queue(self, queue: SqsQueue):
        self.queues[queue.key] = queue
        self.queue_url_index[queue.url] = queue

    def _remove_queue_by_url(self, queue_url: str):
        queue = self.queue_url_index[queue_url]

        del self.queues[queue.key]
        del self.queue_url_index[queue_url]

    @handler("CreateQueue")
    def create_queue(
        self,
        context: RequestContext,
        queue_name: String,
        attributes: QueueAttributeMap = None,
        tags: TagMap = None,
    ) -> CreateQueueResult:
        assert_queue_name(queue_name)

        k = QueueKey(context.region, context.account_id, queue_name)

        if k in self.queues:
            raise QueueNameExists(queue_name)

        queue = SqsQueue(k, attributes, tags)
        self._add_queue(queue)

        return CreateQueueResult(QueueUrl=queue.url)

    @handler("GetQueueUrl")
    def get_queue_url(
        self, context: RequestContext, queue_name: String, queue_owner_aws_account_id: String = None
    ) -> GetQueueUrlResult:
        account_id = queue_owner_aws_account_id or context.account_id
        key = QueueKey(context.region, account_id, queue_name)

        if key not in self.queues:
            raise QueueDoesNotExist("The specified queue does not exist for this wsdl version.")

        return GetQueueUrlResult(QueueUrl=self.queues[key].url)

    @handler("ListQueues")
    def list_queues(
        self,
        context: RequestContext,
        queue_name_prefix: String = None,
        next_token: Token = None,
        max_results: BoxedInteger = None,
    ) -> ListQueuesResult:
        urls = list()

        for queue in self.queues.values():
            LOG.info("queue: %s: %s", queue, queue.key)

            if queue.key.region != context.region:
                continue
            if queue.key.account_id != context.account_id:
                continue
            if queue_name_prefix:
                if not queue.name.startswith(queue_name_prefix):
                    continue
            urls.append(queue.url)

        if max_results:
            urls = urls[:max_results]

        return ListQueuesResult(QueueUrls=urls)

    @handler("DeleteQueue")
    def delete_queue(self, context: RequestContext, queue_url: String) -> None:
        if queue_url not in self.queue_url_index:
            raise NonExistentQueue()

        self._remove_queue_by_url(queue_url)

    @handler("GetQueueAttributes")
    def get_queue_attributes(
        self, context: RequestContext, queue_url: String, attribute_names: AttributeNameList = None
    ) -> GetQueueAttributesResult:

        if queue_url not in self.queue_url_index:
            raise NonExistentQueue()

        queue = self.queue_url_index[queue_url]

        LOG.info("getting queue attributes: %s", attribute_names)
        if not attribute_names:
            return GetQueueAttributesResult(Attributes=dict())

        if QueueAttributeName.All in attribute_names:
            return GetQueueAttributesResult(Attributes=queue.attributes)

        result: Dict[QueueAttributeName, str] = dict()

        for attr in attribute_names:
            try:
                getattr(QueueAttributeName, attr)
            except AttributeError:
                raise InvalidAttributeName("Unknown attribute %s." % attr)

            result[attr] = queue.attributes.get(attr)

        return GetQueueAttributesResult(Attributes=result)

    @handler("SendMessage")
    def send_message(
        self,
        context: RequestContext,
        queue_url: String,
        message_body: String,
        delay_seconds: Integer = None,
        message_attributes: MessageBodyAttributeMap = None,  # TODO
        message_system_attributes: MessageBodySystemAttributeMap = None,  # TODO
        message_deduplication_id: String = None,  # TODO
        message_group_id: String = None,  # TODO
    ) -> SendMessageResult:

        if queue_url not in self.queue_url_index:
            # FIXME: cannot return exception that aren't in the spec
            raise NonExistentQueue()

        queue = self.queue_url_index[queue_url]

        message: Message = Message(
            MessageId=generate_message_id(),
            MD5OfBody=md5(message_body),
            Body=message_body,
            Attributes=message_system_attributes,
            MD5OfMessageAttributes=None,
            MessageAttributes=message_attributes,
        )

        delay_seconds = delay_seconds or queue.attributes.get(QueueAttributeName.DelaySeconds, 0)
        if delay_seconds:
            # FIXME: this is a pretty bad implementation (one thread per message...)
            threading.Timer(int(delay_seconds), queue.messages.put_nowait, args=(message,)).start()
        else:
            queue.messages.put_nowait(message)

        return SendMessageResult(
            MessageId=message["MessageId"],
            MD5OfMessageBody=message["MD5OfBody"],
            MD5OfMessageAttributes=None,  # TODO
            SequenceNumber=None,  # TODO
            MD5OfMessageSystemAttributes=None,  # TODO
        )

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
        if queue_url not in self.queue_url_index:
            raise QueueDoesNotExist("The specified queue does not exist for this wsdl version.")

        queue = self.queue_url_index[queue_url]

        num = max_number_of_messages or 1
        # collect messages
        messages = list()
        while num:
            if wait_time_seconds is None:
                try:
                    msg = queue.messages.get_nowait()
                except Empty:
                    break
            else:
                try:
                    msg = queue.messages.get(timeout=wait_time_seconds)
                except Empty:
                    break
            msg["ReceiptHandle"] = generate_receipt_handle()
            messages.append(msg)
            num -= 1

        return ReceiveMessageResult(Messages=messages)
