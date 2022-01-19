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
    def __init__(self, aws_access_key_id, aws_secret_access_key, lambda_upload_bucket, lambda_iam_role, region_name):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.lambda_upload_bucket = lambda_upload_bucket
        self.lambda_iam_role = lambda_iam_role
        self.region_name = region_name

    def create_aws_client(self, client_type):
        if client_type not in ['lambda', 's3']:
            raise Exception("Undefined client type, please use 'lambda' or 's3'.")
        aws_session = boto3.Session(aws_access_key_id = self.aws_access_key_id, aws_secret_access_key = self.aws_secret_access_key, region_name = self.region_name)
        client = aws_session.client(client_type, self.region_name)
        return client
    
    @staticmethod
    def _updateZip(zip_path, filename):
        """ 
        zip = zipfile.ZipFile(zip_path,'a')
        zip.write(filename, os.path.basename(filename))
        zip.close() 
        """
        if not os.path.exists(zip_path):
            raise Exception("Missing zip file: " + zip_path, "Fatal")
        if not os.path.exists(filename):
            raise Exception("Missing file: " + filename, "Fatal")

        tempdir = tempfile.mkdtemp()
        try:
            tempname = os.path.join(tempdir, 'new.zip')
            with zipfile.ZipFile(zip_path, 'r') as zipread:
                with zipfile.ZipFile(tempname, 'w') as zipwrite:
                    for item in zipread.infolist():
                        if item.filename != filename:
                            data = zipread.read(item.filename)
                            zipwrite.writestr(item, data)
            shutil.move(tempname, zip_path)
        finally:
            shutil.rmtree(tempdir)

        with zipfile.ZipFile(zip_path, 'a') as z:
            z.write(filename)
        """ 
        tempdir = tempfile.mkdtemp() 
        with zipfile.ZipFile(zip_path, 'r') as old_zip:
            old_zip.extractall(tempdir)
        os.remove(zip_path)
        shutil.copyfile(filename, os.path.join(tempdir, os.path.basename(filename)))

        with zipfile.ZipFile(zip_path, 'w') as new_zip:
            for folderName, subfolders, filenames in os.walk(tempdir):
                for filename in filenames:
                    filePath = os.path.join(folderName, filename)
                    new_zip.write(filePath, os.path.basename(filePath)) 
        """

    def create_lambda_function(self, function_name):
        s3_client = self.create_aws_client('s3')
        self._updateZip('base.zip', 'aws_lambda_func_def.py')
        s3_client.put_object(Key='base.zip', \
                            Bucket=self.lambda_upload_bucket, \
                            Body=open('base.zip', 'rb') \
                            )

        lambda_client = self.create_aws_client('lambda')
        lambda_client.create_function(
            FunctionName=function_name,
            Runtime='python3.7',
            Role=self.lambda_iam_role,
            Handler='process.handler',
            Code={
                'S3Bucket': self.lambda_upload_bucket,
                'S3Key': 'base.zip'
            },
            Description='',
            Timeout=300,
            MemorySize=1024,
            Publish=True
        )

    def call_lambda_function(self, function_name, request):
        lambda_client = self.create_aws_client('lambda')
        response = lambda_client.invoke(
                FunctionName=function_name,
                InvocationType='RequestResponse',
                LogType='Tail',
                Payload=json.dumps(request))

        response_payload = json.loads(response['Payload'].read())
        ret_type = response_payload['ret_type']
        if(ret_type == 'key'):  #   The output file is saved to another bucket (s3_output_bucket).
            ret_value = response_payload['ret_value']
            print("The window given is too large, can't be returned immediately,\nPlease check {}".format(ret_value))
        elif(ret_type == 'content'):
            ret_value = base64.b64decode(response_payload['ret_value'].encode("utf-8"))
        else:
            raise Exception("Undefined return type when calling " + function, "Fatal")
        
        ret_dict = {'return_type':ret_type, 'ret_value': ret_value}
        return ret_dict