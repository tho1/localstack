from botocore.model import ServiceModel
from requests.models import Response

from localstack.aws.api import HttpRequest, RequestContext
from localstack.aws.skeleton import Skeleton
from localstack.aws.spec import load_service
from localstack.services.generic_proxy import ProxyListener


class AwsApiListener(ProxyListener):
    service: ServiceModel

    def __init__(self, api, delegate):
        self.service = load_service(api)
        self.skeleton = Skeleton(self.service, delegate)

    def forward_request(self, method, path, data, headers):
        request = HttpRequest(
            method=method,
            path=path,
            headers=headers,
            body=data,
        )

        context = RequestContext()
        context.service = self.service
        context.request = request
        context.region = None  # FIXME
        context.account = None  # FIXME

        response = self.skeleton.invoke(context)

        # TODO: this is ugly, but it's the way that the edge proxy expects responses. again, using HTTP server framework
        #  response models would be better.
        resp = Response()
        resp._content = response["body"]
        resp.status_code = response["status_code"]
        resp.headers.update(response["headers"])

        return resp
