import copy
import inspect
import logging
import random
import re
import string
import threading
from queue import Empty
from queue import Queue as FifoQueue
from typing import Dict, List, NamedTuple, Set

from localstack.aws.api import CommonServiceException, RequestContext
from localstack.aws.api.sqs import (
    ActionNameList,
    AttributeNameList,
    AWSAccountIdList,
    BoxedInteger,
    CreateQueueResult,
    GetQueueAttributesResult,
    GetQueueUrlResult,
    Integer,
    InvalidAttributeName,
    ListQueuesResult,
    ListQueueTagsResult,
    Message,
    MessageAttributeNameList,
    MessageBodyAttributeMap,
    MessageBodySystemAttributeMap,
    MessageSystemAttributeName,
    PurgeQueueInProgress,
    QueueAttributeMap,
    QueueAttributeName,
    QueueDoesNotExist,
    QueueNameExists,
    ReceiveMessageResult,
    SendMessageResult,
    SqsApi,
    String,
    TagKeyList,
    TagMap,
    Token,
)
from localstack.aws.spec import load_service
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


class Permission(NamedTuple):
    # TODO: just a placeholder for real policies
    label: str
    account_id: str
    action: str


class SqsQueue:
    key: QueueKey

    attributes: QueueAttributeMap
    tags: TagMap
    message: List[Message]
    permissions: Set[Permission]

    purge_in_progress: bool

    def __init__(self, key: QueueKey, attributes=None, tags=None) -> None:
        super().__init__()
        self.key = key
        self.tags = tags or dict()
        self.messages = FifoQueue()

        self.attributes = self.default_attributes()
        if attributes:
            self.attributes.update(attributes)

        self.purge_in_progress = False
        self.permissions = set()

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

    def update_last_modified(self, timestamp: int = None):
        if timestamp is None:
            timestamp = now()

        self.attributes[QueueAttributeName.LastModifiedTimestamp] = str(timestamp)

    @property
    def name(self):
        return self.key.name

    @property
    def owner(self):
        return self.key.account_id

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
    """
    LocalStack SQS Provider.

    LIMITATIONS:
        - Calculation of message attribute MD5 hash.
        - Message visiibility
        - Message deletion
        - Pagination of results (NextToken)
        - Sequence numbering
        - Delivery guarantees
        - Permissions are not real IAM policies
    """

    queues: Dict[QueueKey, SqsQueue]
    queue_url_index: Dict[str, SqsQueue]

    def __init__(self) -> None:
        super().__init__()
        self.queues = dict()
        self.queue_url_index = dict()
        self._mutex = threading.RLock()

    def _add_queue(self, queue: SqsQueue):
        with self._mutex:
            self.queues[queue.key] = queue
            self.queue_url_index[queue.url] = queue

    def _require_queue_by_url(self, queue_url: str) -> SqsQueue:
        """
        Returns the queue for the given url, or raises a NonExistentQueue error.

        :param queue_url: The QueueUrl
        :returns: the queue
        :raises NonExistentQueue: if the queue does not exist
        """
        with self._mutex:
            try:
                return self.queue_url_index[queue_url]
            except KeyError:
                raise NonExistentQueue()

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

    def get_queue_url(
        self, context: RequestContext, queue_name: String, queue_owner_aws_account_id: String = None
    ) -> GetQueueUrlResult:
        account_id = queue_owner_aws_account_id or context.account_id
        key = QueueKey(context.region, account_id, queue_name)

        if key not in self.queues:
            raise QueueDoesNotExist("The specified queue does not exist for this wsdl version.")

        queue = self.queues[key]
        self._assert_permission(context, queue)

        return GetQueueUrlResult(QueueUrl=queue.url)

    def list_queues(
        self,
        context: RequestContext,
        queue_name_prefix: String = None,
        next_token: Token = None,
        max_results: BoxedInteger = None,
    ) -> ListQueuesResult:
        urls = list()

        for queue in self.queues.values():
            if queue.key.region != context.region:
                continue
            if queue.key.account_id != context.account_id:
                continue
            if queue_name_prefix:
                if not queue.name.startswith(queue_name_prefix):
                    continue
            urls.append(queue.url)

        if max_results:
            # FIXME: also need to solve pagination with stateful iterators: If the total number of items available is
            #  more than the value specified, a NextToken is provided in the command's output. To resume pagination,
            #  provide the NextToken value in the starting-token argument of a subsequent command. Do not use the
            #  NextToken response element directly outside of the AWS CLI.
            urls = urls[:max_results]

        return ListQueuesResult(QueueUrls=urls)

    def delete_queue(self, context: RequestContext, queue_url: String) -> None:
        with self._mutex:
            queue = self._require_queue_by_url(queue_url)
            self._assert_permission(context, queue)
            del self.queues[queue.key]
            del self.queue_url_index[queue_url]

    def get_queue_attributes(
        self, context: RequestContext, queue_url: String, attribute_names: AttributeNameList = None
    ) -> GetQueueAttributesResult:
        queue = self._require_queue_by_url(queue_url)
        self._assert_permission(context, queue)

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
        queue = self._require_queue_by_url(queue_url)
        self._assert_permission(context, queue)

        # TODO: default message attributes (SenderId, ApproximateFirstReceiveTimestamp, ...)

        message: Message = Message(
            MessageId=generate_message_id(),
            MD5OfBody=md5(message_body),
            Body=message_body,
            Attributes=self._create_message_attributes(context, message_system_attributes),
            MD5OfMessageAttributes=None,  # TODO (see Message.attribute_md5 from moto)
            MessageAttributes=message_attributes,
        )

        delay_seconds = delay_seconds or queue.attributes.get(QueueAttributeName.DelaySeconds, 0)
        if delay_seconds:
            # FIXME: this is a pretty bad implementation (one thread per message...). polling on a priority queue
            #  would probably be better.
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
        queue = self._require_queue_by_url(queue_url)
        self._assert_permission(context, queue)

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

            # prepare message for receipt
            msg: Message = copy.deepcopy(msg)
            msg["ReceiptHandle"] = generate_receipt_handle()
            # filter attributes
            if message_attribute_names:
                if "All" not in message_attribute_names:
                    msg["MessageAttributes"] = {
                        k: v
                        for k, v in msg["MessageAttributes"].items()
                        if k in message_attribute_names
                    }
                msg["MD5OfMessageAttributes"] = None  # TODO
            else:
                del msg["MessageAttributes"]

            # add message to result
            messages.append(msg)
            num -= 1

        # TODO: how does receiving behave if the queue was deleted in the meantime?
        return ReceiveMessageResult(Messages=messages)

    def purge_queue(self, context: RequestContext, queue_url: String) -> None:
        queue = self._require_queue_by_url(queue_url)
        self._assert_permission(context, queue)

        with self._mutex:
            # FIXME: use queue-specific locks
            if queue.purge_in_progress:
                raise PurgeQueueInProgress()
            queue.purge_in_progress = True

        # TODO: how do other methods behave when purge is in progress?

        try:
            while True:
                queue.messages.get_nowait()
        except Empty:
            return
        finally:
            queue.purge_in_progress = False

    def set_queue_attributes(
        self, context: RequestContext, queue_url: String, attributes: QueueAttributeMap
    ) -> None:
        queue = self._require_queue_by_url(queue_url)

        if not attributes:
            return

        self._validate_queue_attributes(attributes)

        for k, v in attributes.items():
            queue.attributes[k] = v

    def tag_queue(self, context: RequestContext, queue_url: String, tags: TagMap) -> None:
        queue = self._require_queue_by_url(queue_url)
        self._assert_permission(context, queue)

        if not tags:
            return

        for k, v in tags.items():
            queue.tags[k] = v

    def list_queue_tags(self, context: RequestContext, queue_url: String) -> ListQueueTagsResult:
        queue = self._require_queue_by_url(queue_url)
        self._assert_permission(context, queue)
        return ListQueueTagsResult(Tags=queue.tags)

    def untag_queue(self, context: RequestContext, queue_url: String, tag_keys: TagKeyList) -> None:
        queue = self._require_queue_by_url(queue_url)
        self._assert_permission(context, queue)

        for k in tag_keys:
            if k in queue.tags:
                del queue.tags[k]

    def add_permission(
        self,
        context: RequestContext,
        queue_url: String,
        label: String,
        aws_account_ids: AWSAccountIdList,
        actions: ActionNameList,
    ) -> None:
        queue = self._require_queue_by_url(queue_url)
        self._assert_permission(context, queue)

        self._validate_actions(actions)

        for account_id in aws_account_ids:
            for action in actions:
                queue.permissions.add(Permission(label, account_id, action))

    def remove_permission(self, context: RequestContext, queue_url: String, label: String) -> None:
        queue = self._require_queue_by_url(queue_url)
        self._assert_permission(context, queue)

        candidate = None
        for perm in queue.permissions:
            if perm.label == label:
                candidate = perm
                break
        if candidate:
            queue.permissions.remove(candidate)

    def _create_message_attributes(
        self,
        context: RequestContext,
        message_system_attributes: MessageBodySystemAttributeMap = None,
    ) -> Dict[MessageSystemAttributeName, str]:
        result: Dict[MessageSystemAttributeName, str] = {
            MessageSystemAttributeName.SenderId: context.account_id,
            MessageSystemAttributeName.SentTimestamp: str(now()),
        }

        if message_system_attributes is not None:
            result.update(message_system_attributes)

        return result

    def _validate_queue_attributes(self, attributes: QueueAttributeMap):
        valid = [k[1] for k in inspect.getmembers(QueueAttributeName)]

        for k in attributes.keys():
            if k not in valid:
                raise InvalidAttributeName("Unknown attribute name %s" % k)

    def _validate_actions(self, actions: ActionNameList):
        service = load_service(service=self.service, version=self.version)
        # FIXME: this is a bit of a heuristic as it will also include actions like "ListQueues" which is not
        #  associated with an action on a queue
        valid = list(service.operation_names)
        valid.append("*")

        for action in actions:
            if action not in valid:
                raise InvalidParameterValues(
                    f"Value SQS:{action} for parameter ActionName is invalid. Reason: Please refer to the appropriate "
                    "WSDL for a list of valid actions. "
                )

    def _assert_permission(self, context: RequestContext, queue: SqsQueue):
        action = context.operation.name
        account_id = context.account_id

        if account_id == queue.owner:
            return

        for permission in queue.permissions:
            if permission.account_id != account_id:
                continue
            if permission.action == "*":
                return
            if permission.action == action:
                return

        raise CommonServiceException("AccessDeniedException", "Not allowed (TODO: correct message)")
