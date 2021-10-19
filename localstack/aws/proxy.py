from botocore.model import ServiceModel
from requests.models import Response

from localstack.aws.api import RequestContext
from localstack.aws.protocol.parser import RequestDict
from localstack.aws.skeleton import Skeleton
from localstack.aws.spec import load_service
from localstack.services.generic_proxy import ProxyListener


class AwsApiListener(ProxyListener):
    service: ServiceModel

    def __init__(self, api, delegate):
        self.service = load_service(api)
        self.skeleton = Skeleton(self.service, delegate)

    def forward_request(self, method, path, data, headers):
        # TODO: ideally this would be something more flexible than just a dict, for example the actual unaltered
        #  werkzeug server (or whatever framework) http request.
        request = RequestDict(
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

        result = self.skeleton.invoke(context)

        # TODO: this is ugly, but it's the way that the edge proxy expects responses. again, using HTTP server framework
        #  response models would be better.
        resp = Response()
        resp._content = result["body"]
        resp.status_code = result["status_code"]
        resp.headers.update(result["headers"])

        return resp
