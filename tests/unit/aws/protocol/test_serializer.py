from botocore.parsers import QueryParser

from localstack.aws.protocol.serializer import QueryResponseSerializer
from localstack.aws.spec import load_service


def _test_query_serializer_botocore_integration_test(service: str, action: str, response: dict):
    # Load the appropriate service
    service = load_service(service)

    # Use our serializer to serialize the response
    response_serializer = QueryResponseSerializer()
    serialized_response = response_serializer.serialize_to_response(
        response, service.operation_model(action)
    )

    # Use the parser from botocore to parse the serialized response
    response_parser = QueryParser()
    parsed_response = response_parser.parse(
        serialized_response, service.operation_model(action).output_shape
    )

    # Check if the result is equal to the initial response params
    assert parsed_response == response


def test_query_parser_cloudformation_with_botocore():
    parameters = {
        "StackResourceDrift": {
            "StackId": "arn:aws:cloudformation:us-west-2:123456789012:stack/MyStack/d0a825a0-e4cd-xmpl-b9fb-061c69e99204",
            "LogicalResourceId": "MyFunction",
            "PhysicalResourceId": "my-function-SEZV4XMPL4S5",
            "ResourceType": "AWS::Lambda::Function",
            "ExpectedProperties": '{"Description":"Write a file to S3.","Environment":{"Variables":{"bucket":"my-stack-bucket-1vc62xmplgguf"}},"Handler":"index.handler","MemorySize":128,"Role":"arn:aws:iam::123456789012:role/my-functionRole-HIZXMPLEOM9E","Runtime":"nodejs10.x","Tags":[{"Key":"lambda:createdBy","Value":"SAM"}],"Timeout":900,"TracingConfig":{"Mode":"Active"}}',
            "ActualProperties": '{"Description":"Write a file to S3.","Environment":{"Variables":{"bucket":"my-stack-bucket-1vc62xmplgguf"}},"Handler":"index.handler","MemorySize":256,"Role":"arn:aws:iam::123456789012:role/my-functionRole-HIZXMPLEOM9E","Runtime":"nodejs10.x","Tags":[{"Key":"lambda:createdBy","Value":"SAM"}],"Timeout":22,"TracingConfig":{"Mode":"Active"}}',
            "PropertyDifferences": [
                {
                    "PropertyPath": "/MemorySize",
                    "ExpectedValue": "128",
                    "ActualValue": "256",
                    "DifferenceType": "NOT_EQUAL",
                },
                {
                    "PropertyPath": "/Timeout",
                    "ExpectedValue": "900",
                    "ActualValue": "22",
                    "DifferenceType": "NOT_EQUAL",
                },
            ],
            "StackResourceDriftStatus": "MODIFIED",
            "Timestamp": "2019-10-02T05:58:47.433Z",
        }
    }
    _test_query_serializer_botocore_integration_test(
        "cloudformation", "DetectStackResourceDrift", parameters
    )


# TODO Add additional tests (or even automate the creation)
# - Go to the AWS CLI reference (https://docs.aws.amazon.com)
# - Look at the CLI reference for APIs that use the protocol you want to test
# - Use the output examples to verify that the serialization works
