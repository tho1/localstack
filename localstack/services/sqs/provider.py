import logging
import random
import re
from queue import Empty
from queue import Queue as FifoQueue
from typing import Dict, List, NamedTuple

from localstack.aws.api import RequestContext, handler
from localstack.aws.api.sqs import (
    AttributeNameList,
    BoxedInteger,
    CreateQueueResult,
    GetQueueUrlResult,
    Integer,
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
from localstack.utils.common import md5

LOG = logging.getLogger(__name__)


def get_random_hex(length=8):
    chars = list(range(10)) + ["a", "b", "c", "d", "e", "f"]
    return "".join(str(random.choice(chars)) for x in range(length))


def get_random_message_id():
    return "{0}-{1}-{2}-{3}-{4}".format(
        get_random_hex(8),
        get_random_hex(4),
        get_random_hex(4),
        get_random_hex(4),
        get_random_hex(12),
    )


def assert_queue_name(queue_name: str):
    if not re.match(r"^[a-zA-Z0-9_-]{1,80}$", queue_name):
        # FIXME: InvalidParameterValues is an exception that is not defined in the spec
        raise ValueError(
            "Can only include alphanumeric characters, hyphens, or underscores. 1 to 80 in length",
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
        self.attributes = attributes or dict()
        self.tags = tags or dict()
        self.messages = FifoQueue()

    @property
    def name(self):
        return self.key.name

    @property
    def arn(self) -> str:
        return ""

    @property
    def url(self) -> str:
        return "{host}/{account_id}/{name}".format(
            host=get_edge_url(),  # FIXME region
            account_id=self.key.account_id,
            name=self.key.name,
        )


class SqsProvider(SqsApi):
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

        queue = SqsQueue(k)
        queue.tags = tags
        queue.attributes = attributes
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

        LOG.info("list queues: %s", urls)
        return ListQueuesResult(QueueUrls=urls)

    def delete_queue(self, context: RequestContext, queue_url: String) -> None:
        if queue_url not in self.queue_url_index:
            raise QueueDoesNotExist(queue_url)

        pass

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

        if queue_url not in self.queue_url_index:
            # FIXME: cannot return exception that aren't in the spec
            raise QueueDoesNotExist("The specified queue does not exist for this wsdl version.")

        queue = self.queue_url_index[queue_url]

        delay_seconds = delay_seconds or queue.attributes.get(QueueAttributeName.DelaySeconds)
        # TODO: implement delay_seconds

        message: Message = Message(
            MessageId=get_random_message_id(),
            MD5OfBody=md5(message_body),
            Body=message_body,
            Attributes=message_system_attributes,
            MD5OfMessageAttributes=None,
            MessageAttributes=message_attributes,
        )

        queue.messages.put_nowait(message)

        return SendMessageResult(
            MessageId=message["MessageId"],
            MD5OfMessageBody=message["MD5OfBody"],
            MD5OfMessageAttributes=message["MD5OfMessageAttributes"],
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
            messages.append(msg)
            num -= 1

        return ReceiveMessageResult(Messages=messages)
