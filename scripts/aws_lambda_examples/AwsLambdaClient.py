import base64
import binascii
import boto3
import os
import zipfile
import tempfile
import shutil
import json


# Settings for aws lambda uploading and calling.
class AwsLambdaClient:
    """
    A class to store the aws user info, and to handle the lambda related functions,
    including creating a lambda function, and calling a lambda function.
    """

    def __init__(
        self,
        aws_access_key_id,
        aws_secret_access_key,
        lambda_upload_bucket,
        lambda_iam_role,
        region_name,
    ):
        """
        Basic constructor for AwsLambdaClient, require the users' aws info. Those info would be used
        to establish aws connection and create s3 and lambda clients.

        :param aws_access_key_id: A part of the credentials to authenticate the user
        :param aws_secret_access_key: A part of the credentials to authenticate the user
        :param lambda_upload_bucket: The name of s3 bucket where the zip file containing lambda function would be uploaded to
        :param lambda_iam_role: The IAM role that has lambda and s3 permission, required to create lambda function.
        :param region_name: AWS region where the lambda function will be deployed.
        """
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.lambda_upload_bucket = lambda_upload_bucket
        self.lambda_iam_role = lambda_iam_role
        self.region_name = region_name

    def create_aws_client(self, client_type):
        """
        Establish the connection with AWS, and create the client.

        :param client_type: The service name user would like to use, in this class, only 'lambda' and 's3' are supported.
        :return: The lambda or s3 client.
        """
        if client_type not in ["lambda", "s3"]:
            raise Exception("Undefined client type, please use 'lambda' or 's3'.")
        aws_session = boto3.Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name,
        )
        return aws_session.client(client_type)

    @staticmethod
    def _updateZip(zip_path, filename):
        """
        Update a file in the zip archive. This function has the same functionaliy as the command "zip -r ${zip_path} ${filename}".

        :param zip_path: The path of zip archive.
        :param filename: The path of file to pack in the zip archive and update.
        """
        if not os.path.exists(zip_path):
            raise Exception("Missing zip file: " + zip_path, "Fatal")
        if not os.path.exists(filename):
            raise Exception("Missing file: " + filename, "Fatal")

        tempdir = tempfile.mkdtemp()
        try:
            tempname = os.path.join(tempdir, "new.zip")
            with zipfile.ZipFile(zip_path, "r") as zipread:
                with zipfile.ZipFile(tempname, "w") as zipwrite:
                    for item in zipread.infolist():
                        if item.filename != filename:
                            data = zipread.read(item.filename)
                            zipwrite.writestr(item, data)
            shutil.move(tempname, zip_path)
        finally:
            shutil.rmtree(tempdir)

        with zipfile.ZipFile(zip_path, "a") as z:
            z.write(filename)

    def create_lambda_function(self, function_name):
        """
        Upload a user-defined lambda function to the AWS.

        :param functionName: the name of the lambda function, which will be used to call
        """

        s3_client = self.create_aws_client("s3")
        self._updateZip("base.zip", "process.py")
        self._updateZip("base.zip", "aws_lambda_func_def.py")
        with open("base.zip", "rb") as archive:
            s3_client.put_object(
                Key="base.zip",
                Bucket=self.lambda_upload_bucket,
                Body=archive,
            )

        lambda_client = self.create_aws_client("lambda")
        lambda_client.create_function(
            FunctionName=function_name,
            Runtime="python3.13",
            Role=self.lambda_iam_role,
            Handler="process.handler",
            Code={"S3Bucket": self.lambda_upload_bucket, "S3Key": "base.zip"},
            Description="",
            Timeout=300,
            MemorySize=1024,
            Publish=True,
        )

    def call_lambda_function(self, function_name, request):
        """
        Call an existing lambda function.

        :param function_name: the name of the lambda function to call.
        :param request: an dictionary that contains all the arguments passed to the lambda function. It will be dumped as a json string and then used as the payload of request.
        It should at least contain four elements: ‘src_bucket’, ‘dst_bucket’, ‘src_key’, ‘dst_key’. They will indicate the input and output object of this lambda call.
        :return: a dict that contain two elements:
            1) return_type: two possible values: ‘key’ or ‘content’,
                ‘key’ means that the output object is saved to some place in s3.
                ‘content’ means that the output object is directly returned through payload
            2) ret_value:
                If return_type=’key’, ret_value will be the existing ``bucket::key``
                location of the output object in s3.
                If return_type=’content’, ret_value will be the decoded output bytes.
        """

        lambda_client = self.create_aws_client("lambda")
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType="RequestResponse",
            LogType="Tail",
            Payload=json.dumps(request),
        )

        payload_stream = response.get("Payload")
        if payload_stream is None or not hasattr(payload_stream, "read"):
            raise RuntimeError(
                f"Lambda {function_name!r} returned no readable Payload stream"
            )
        try:
            raw_payload = payload_stream.read()
        except Exception as error:
            raise RuntimeError(
                f"Failed to read the response from Lambda {function_name!r}"
            ) from error
        finally:
            close = getattr(payload_stream, "close", None)
            if close is not None:
                close()

        if response.get("FunctionError"):
            raise RuntimeError(
                f"Lambda {function_name!r} failed with FunctionError "
                f"{response['FunctionError']!r}"
            )

        try:
            if isinstance(raw_payload, bytes):
                decoded_payload = raw_payload.decode("utf-8")
            elif isinstance(raw_payload, str):
                decoded_payload = raw_payload
            else:
                raise TypeError(
                    f"Payload.read() returned {type(raw_payload).__name__}, not bytes"
                )
            response_payload = json.loads(decoded_payload)
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError) as error:
            raise RuntimeError(
                f"Lambda {function_name!r} returned malformed UTF-8/JSON"
            ) from error

        if not isinstance(response_payload, dict) or not {
            "ret_type",
            "ret_value",
        }.issubset(response_payload):
            raise RuntimeError(
                f"Lambda {function_name!r} returned a malformed response object"
            )
        ret_type = response_payload["ret_type"]
        encoded_value = response_payload["ret_value"]
        if ret_type == "key":
            if not isinstance(encoded_value, str) or not encoded_value:
                raise RuntimeError(
                    f"Lambda {function_name!r} returned an invalid S3 key"
                )
            ret_value = encoded_value
        elif ret_type == "content":
            if not isinstance(encoded_value, str):
                raise RuntimeError(
                    f"Lambda {function_name!r} returned non-string base64 content"
                )
            try:
                ret_value = base64.b64decode(encoded_value, validate=True)
            except (binascii.Error, ValueError) as error:
                raise RuntimeError(
                    f"Lambda {function_name!r} returned invalid base64 content"
                ) from error
        else:
            raise RuntimeError(
                f"Lambda {function_name!r} returned unknown ret_type {ret_type!r}"
            )

        return {"return_type": ret_type, "ret_value": ret_value}
