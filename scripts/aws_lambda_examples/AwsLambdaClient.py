import base64
import io
import boto3
import sys
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
        client = aws_session.client(client_type, self.region_name)
        return client

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
        self._updateZip("base.zip", "aws_lambda_func_def.py")
        s3_client.put_object(
            Key="base.zip",
            Bucket=self.lambda_upload_bucket,
            Body=open("base.zip", "rb"),
        )

        lambda_client = self.create_aws_client("lambda")
        lambda_client.create_function(
            FunctionName=function_name,
            Runtime="python3.7",
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

        :param functionName: the name of the lambda function to call.
        :param request: an dictionary that contains all the arguments passed to the lambda function. It will be dumped as a json string and then used as the payload of request.
        It should at least contain four elements: ‘src_bucket’, ‘dst_bucket’, ‘src_key’, ‘dst_key’. They will indicate the input and output object of this lambda call.
        :return: a dict that contain two elements:
            1) ret_type: two possible value: ‘key’ or ‘value’,
                ‘key’ means that the output object is saved to some place in s3.
                ‘value’ means that the output object is directly returned through payload
            2) ret_value:
                If ret_type=’key’, ret_value will be the key of the output object in s3.
                If ret_type=’value’, ret_value will be the bytes of the returning object.
        """

        lambda_client = self.create_aws_client("lambda")
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType="RequestResponse",
            LogType="Tail",
            Payload=json.dumps(request),
        )

        response_payload = json.loads(response["Payload"].read())
        ret_type = response_payload["ret_type"]
        if (
            ret_type == "key"
        ):  #   The output file is saved to another bucket (s3_output_bucket).
            ret_value = response_payload["ret_value"]
            print(
                "The window given is too large, can't be returned immediately,\nPlease check {}".format(
                    ret_value
                )
            )
        elif ret_type == "content":
            ret_value = base64.b64decode(response_payload["ret_value"].encode("utf-8"))
        else:
            raise Exception("Undefined return type when calling " + function, "Fatal")

        ret_dict = {"return_type": ret_type, "ret_value": ret_value}
        return ret_dict
