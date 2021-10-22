from datetime import datetime

import pytest
from botocore.parsers import ResponseParser, create_parser
from dateutil.tz import tzutc

from localstack.aws.api import CommonServiceException, ServiceException
from localstack.aws.protocol.serializer import create_serializer
from localstack.aws.spec import load_service


class InvalidMessageContents(ServiceException):
    pass


def _botocore_serializer_integration_test(
    service: str, action: str, response: dict, status_code=200
):
    # Load the appropriate service
    service = load_service(service)

    # Use our serializer to serialize the response
    response_serializer = create_serializer(service)
    serialized_response = response_serializer.serialize_to_response(
        response, service.operation_model(action)
    )

    # Use the parser from botocore to parse the serialized response
    response_parser = create_parser(service.protocol)
    parsed_response = response_parser.parse(
        serialized_response, service.operation_model(action).output_shape
    )

    # Check if the result is equal to the initial response params
    assert "ResponseMetadata" in parsed_response
    assert "HTTPStatusCode" in parsed_response["ResponseMetadata"]
    assert parsed_response["ResponseMetadata"]["HTTPStatusCode"] == status_code
    assert "RequestId" in parsed_response["ResponseMetadata"]
    assert len(parsed_response["ResponseMetadata"]["RequestId"]) == 52
    del parsed_response["ResponseMetadata"]
    assert parsed_response == response


def _botocore_error_serializer_integration_test(
    service: str,
    action: str,
    exception: ServiceException,
    code: str,
    status_code: int,
    message: str,
):
    # Load the appropriate service
    service = load_service(service)

    # Use our serializer to serialize the response
    response_serializer = create_serializer(service)
    serialized_response = response_serializer.serialize_error_to_response(
        exception, service.operation_model(action)
    )

    # Use the parser from botocore to parse the serialized response
    response_parser: ResponseParser = create_parser(service.protocol)
    parsed_response = response_parser.parse(
        serialized_response, service.operation_model(action).output_shape
    )

    # Check if the result is equal to the initial response params
    assert "Error" in parsed_response
    assert "Code" in parsed_response["Error"]
    assert "Message" in parsed_response["Error"]
    assert parsed_response["Error"]["Code"] == code
    assert parsed_response["Error"]["Message"] == message

    assert "ResponseMetadata" in parsed_response
    assert "RequestId" in parsed_response["ResponseMetadata"]
    assert len(parsed_response["ResponseMetadata"]["RequestId"]) == 52
    assert "HTTPStatusCode" in parsed_response["ResponseMetadata"]
    assert parsed_response["ResponseMetadata"]["HTTPStatusCode"] == status_code


def test_rest_xml_serializer_cloudfront_with_botocore():
    parameters = {
        "TestResult": {
            "FunctionSummary": {
                "Name": "string",
                "Status": "string",
                "FunctionConfig": {"Comment": "string", "Runtime": "cloudfront-js-1.0"},
                "FunctionMetadata": {
                    "FunctionARN": "string",
                    "Stage": "LIVE",
                    "CreatedTime": datetime(2015, 1, 1, tzinfo=tzutc()),
                    "LastModifiedTime": datetime(2015, 1, 1, tzinfo=tzutc()),
                },
            },
            "ComputeUtilization": "string",
            "FunctionExecutionLogs": [
                "string",
            ],
            "FunctionErrorMessage": "string",
            "FunctionOutput": "string",
        }
    }
    _botocore_serializer_integration_test("cloudfront", "TestFunction", parameters)


def test_rest_xml_serializer_route53_with_botocore():
    parameters = {
        "HostedZone": {
            "Id": "/hostedzone/9WXI4LV03NAZVS1",
            "Name": "fuu.",
            "Config": {"PrivateZone": False},
            "ResourceRecordSetCount": 0,
        },
        "DelegationSet": {"NameServers": ["dns.localhost.localstack.cloud"]},
    }
    _botocore_serializer_integration_test("route53", "CreateHostedZone", parameters, 201)


def test_rest_xml_serializer_s3_with_botocore():
    parameters = {
        "AnalyticsConfiguration": {
            "Id": "string",
            "Filter": {
                "Prefix": "string",
                "Tag": {"Key": "string", "Value": "string"},
                "And": {
                    "Prefix": "string",
                    "Tags": [
                        {"Key": "string", "Value": "string"},
                    ],
                },
            },
            "StorageClassAnalysis": {
                "DataExport": {
                    "OutputSchemaVersion": "V_1",
                    "Destination": {
                        "S3BucketDestination": {
                            "Format": "CSV",
                            "BucketAccountId": "string",
                            "Bucket": "string",
                            "Prefix": "string",
                        }
                    },
                }
            },
        }
    }
    _botocore_serializer_integration_test("s3", "GetBucketAnalyticsConfiguration", parameters)


@pytest.mark.skip(reason="failing! 'Body' has 'streaming=True'!")
def test_rest_xml_serializer_s3_2_with_botocore():
    parameters = {
        # TODO
        # 'Body': StreamingBody(),
        "Body": "Fuu",
        "DeleteMarker": True,
        "AcceptRanges": "string",
        "Expiration": "string",
        "Restore": "string",
        "LastModified": datetime(2015, 1, 1),
        "ContentLength": 123,
        "ETag": "string",
        "MissingMeta": 123,
        "VersionId": "string",
        "CacheControl": "string",
        "ContentDisposition": "string",
        "ContentEncoding": "string",
        "ContentLanguage": "string",
        "ContentRange": "string",
        "ContentType": "string",
        "Expires": datetime(2015, 1, 1),
        "WebsiteRedirectLocation": "string",
        "ServerSideEncryption": "AES256",
        "Metadata": {"string": "string"},
        "SSECustomerAlgorithm": "string",
        "SSECustomerKeyMD5": "string",
        "SSEKMSKeyId": "string",
        "BucketKeyEnabled": True | False,
        "StorageClass": "STANDARD",
        "RequestCharged": "requester",
        "ReplicationStatus": "COMPLETE",
        "PartsCount": 123,
        "TagCount": 123,
        "ObjectLockMode": "GOVERNANCE",
        "ObjectLockRetainUntilDate": datetime(2015, 1, 1),
        "ObjectLockLegalHoldStatus": "ON",
    }
    _botocore_serializer_integration_test("s3", "GetObject", parameters)


def test_query_serializer_cloudformation_with_botocore():
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
            "Timestamp": datetime(2015, 1, 1, tzinfo=tzutc()),
        }
    }
    _botocore_serializer_integration_test("cloudformation", "DetectStackResourceDrift", parameters)


def test_query_serializer_redshift_with_botocore():
    parameters = {
        "Marker": "string",
        "ClusterDbRevisions": [
            {
                "ClusterIdentifier": "string",
                "CurrentDatabaseRevision": "string",
                "DatabaseRevisionReleaseDate": datetime(2015, 1, 1, tzinfo=tzutc()),
                "RevisionTargets": [
                    {
                        "DatabaseRevision": "string",
                        "Description": "string",
                        "DatabaseRevisionReleaseDate": datetime(2015, 1, 1, tzinfo=tzutc()),
                    },
                ],
            },
        ],
    }
    _botocore_serializer_integration_test("redshift", "DescribeClusterDbRevisions", parameters)


def test_query_serializer_sqs_flattened_list_with_botocore():
    response = {
        "QueueUrls": [
            "http://localhost:4566/000000000000/myqueue1",
            "http://localhost:4566/000000000000/myqueue2",
        ]
    }
    _botocore_serializer_integration_test("sqs", "ListQueues", response)


def test_query_protocol_error_serialization():
    exception = InvalidMessageContents("Exception message!")
    _botocore_error_serializer_integration_test(
        "sqs", "SendMessage", exception, "InvalidMessageContents", 400, "Exception message!"
    )


def test_query_protocol_custom_error_serialization():
    exception = CommonServiceException("InvalidParameterValue", "Parameter x was invalid!")
    _botocore_error_serializer_integration_test(
        "sqs", "SendMessage", exception, "InvalidParameterValue", 400, "Parameter x was invalid!"
    )


def test_restxml_protocol_error_serialization():
    class CloudFrontOriginAccessIdentityAlreadyExists(ServiceException):
        pass

    exception = CloudFrontOriginAccessIdentityAlreadyExists("Exception message!")
    _botocore_error_serializer_integration_test(
        "cloudfront",
        "CreateCloudFrontOriginAccessIdentity",
        exception,
        "CloudFrontOriginAccessIdentityAlreadyExists",
        409,
        "Exception message!",
    )


def test_restxml_protocol_custom_error_serialization():
    exception = CommonServiceException(
        "APIAccessCensorship",
        "You shall not access this API! Sincerly, your friendly neighbourhood firefighter.",
        status_code=451,
    )
    _botocore_error_serializer_integration_test(
        "cloudfront",
        "CreateCloudFrontOriginAccessIdentity",
        exception,
        "APIAccessCensorship",
        451,
        "You shall not access this API! Sincerly, your friendly neighbourhood firefighter.",
    )


def test_json_protocol_error_serialization():
    class UserPoolTaggingException(ServiceException):
        pass

    exception = UserPoolTaggingException("Exception message!")
    _botocore_error_serializer_integration_test(
        "cognito-idp",
        "CreateUserPool",
        exception,
        "UserPoolTaggingException",
        400,
        "Exception message!",
    )


def test_json_protocol_custom_error_serialization():
    exception = CommonServiceException(
        "APIAccessCensorship",
        "You shall not access this API! Sincerly, your friendly neighbourhood firefighter.",
        status_code=451,
    )
    _botocore_error_serializer_integration_test(
        "cognito-idp",
        "CreateUserPool",
        exception,
        "APIAccessCensorship",
        451,
        "You shall not access this API! Sincerly, your friendly neighbourhood firefighter.",
    )


def test_json_serializer_cognito_with_botocore():
    parameters = {
        "UserPool": {
            "Id": "string",
            "Name": "string",
            "Policies": {
                "PasswordPolicy": {
                    "MinimumLength": 123,
                    "RequireUppercase": True,
                    "RequireLowercase": True,
                    "RequireNumbers": True,
                    "RequireSymbols": True,
                    "TemporaryPasswordValidityDays": 123,
                }
            },
            "LambdaConfig": {
                "PreSignUp": "string",
                "CustomMessage": "string",
                "PostConfirmation": "string",
                "PreAuthentication": "string",
                "PostAuthentication": "string",
                "DefineAuthChallenge": "string",
                "CreateAuthChallenge": "string",
                "VerifyAuthChallengeResponse": "string",
                "PreTokenGeneration": "string",
                "UserMigration": "string",
                "CustomSMSSender": {"LambdaVersion": "V1_0", "LambdaArn": "string"},
                "CustomEmailSender": {"LambdaVersion": "V1_0", "LambdaArn": "string"},
                "KMSKeyID": "string",
            },
            "Status": "Enabled",
            "LastModifiedDate": datetime(2015, 1, 1, tzinfo=tzutc()),
            "CreationDate": datetime(2015, 1, 1, tzinfo=tzutc()),
            "SchemaAttributes": [
                {
                    "Name": "string",
                    "AttributeDataType": "String",
                    "DeveloperOnlyAttribute": True,
                    "Mutable": True,
                    "Required": True,
                    "NumberAttributeConstraints": {"MinValue": "string", "MaxValue": "string"},
                    "StringAttributeConstraints": {"MinLength": "string", "MaxLength": "string"},
                },
            ],
            "AutoVerifiedAttributes": [
                "phone_number",
            ],
            "AliasAttributes": [
                "phone_number",
            ],
            "UsernameAttributes": [
                "phone_number",
            ],
            "SmsVerificationMessage": "string",
            "EmailVerificationMessage": "string",
            "EmailVerificationSubject": "string",
            "VerificationMessageTemplate": {
                "SmsMessage": "string",
                "EmailMessage": "string",
                "EmailSubject": "string",
                "EmailMessageByLink": "string",
                "EmailSubjectByLink": "string",
                "DefaultEmailOption": "CONFIRM_WITH_LINK",
            },
            "SmsAuthenticationMessage": "string",
            "MfaConfiguration": "OFF",
            "DeviceConfiguration": {
                "ChallengeRequiredOnNewDevice": True,
                "DeviceOnlyRememberedOnUserPrompt": True,
            },
            "EstimatedNumberOfUsers": 123,
            "EmailConfiguration": {
                "SourceArn": "string",
                "ReplyToEmailAddress": "string",
                "EmailSendingAccount": "COGNITO_DEFAULT",
                "From": "string",
                "ConfigurationSet": "string",
            },
            "SmsConfiguration": {"SnsCallerArn": "string", "ExternalId": "string"},
            "UserPoolTags": {"string": "string"},
            "SmsConfigurationFailure": "string",
            "EmailConfigurationFailure": "string",
            "Domain": "string",
            "CustomDomain": "string",
            "AdminCreateUserConfig": {
                "AllowAdminCreateUserOnly": True,
                "UnusedAccountValidityDays": 123,
                "InviteMessageTemplate": {
                    "SMSMessage": "string",
                    "EmailMessage": "string",
                    "EmailSubject": "string",
                },
            },
            "UserPoolAddOns": {"AdvancedSecurityMode": "OFF"},
            "UsernameConfiguration": {"CaseSensitive": True},
            "Arn": "string",
            "AccountRecoverySetting": {
                "RecoveryMechanisms": [
                    {"Priority": 123, "Name": "verified_email"},
                ]
            },
        }
    }
    _botocore_serializer_integration_test("cognito-idp", "DescribeUserPool", parameters)


def test_restjson_protocol_error_serialization():
    class ThrottledException(ServiceException):
        pass

    exception = ThrottledException("Exception message!")
    _botocore_error_serializer_integration_test(
        "xray",
        "UpdateSamplingRule",
        exception,
        "ThrottledException",
        429,
        "Exception message!",
    )


def test_restjson_protocol_custom_error_serialization():
    exception = CommonServiceException(
        "APIAccessCensorship",
        "You shall not access this API! Sincerly, your friendly neighbourhood firefighter.",
        status_code=451,
    )
    _botocore_error_serializer_integration_test(
        "xray",
        "UpdateSamplingRule",
        exception,
        "APIAccessCensorship",
        451,
        "You shall not access this API! Sincerly, your friendly neighbourhood firefighter.",
    )


def test_restjson_serializer_xray_with_botocore():
    parameters = {
        "SamplingRuleRecord": {
            "SamplingRule": {
                "RuleName": "string",
                "RuleARN": "123456789001234567890",
                "ResourceARN": "123456789001234567890",
                "Priority": 123,
                "FixedRate": 123.0,
                "ReservoirSize": 123,
                "ServiceName": "string",
                "ServiceType": "string",
                "Host": "string",
                "HTTPMethod": "string",
                "URLPath": "string",
                "Version": 123,
                "Attributes": {"string": "string"},
            },
            "CreatedAt": datetime(2015, 1, 1, tzinfo=tzutc()),
            "ModifiedAt": datetime(2015, 1, 1, tzinfo=tzutc()),
        }
    }

    _botocore_serializer_integration_test("xray", "UpdateSamplingRule", parameters)


def test_ec2_serializer_ec2_with_botocore():
    parameters = {
        "InstanceEventWindow": {
            "InstanceEventWindowId": "string",
            "TimeRanges": [
                {
                    "StartWeekDay": "sunday",
                    "StartHour": 123,
                    "EndWeekDay": "sunday",
                    "EndHour": 123,
                },
            ],
            "Name": "string",
            "CronExpression": "string",
            "AssociationTarget": {
                "InstanceIds": [
                    "string",
                ],
                "Tags": [
                    {"Key": "string", "Value": "string"},
                ],
                "DedicatedHostIds": [
                    "string",
                ],
            },
            "State": "creating",
            "Tags": [
                {"Key": "string", "Value": "string"},
            ],
        }
    }

    _botocore_serializer_integration_test("ec2", "CreateInstanceEventWindow", parameters)


# TODO Add additional tests (or even automate the creation)
# - Go to the AWS CLI reference (https://docs.aws.amazon.com)
# - Look at the CLI reference for APIs that use the protocol you want to test
# - Use the output examples to verify that the serialization works
