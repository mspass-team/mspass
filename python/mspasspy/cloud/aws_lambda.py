import base64
import io
import boto3
import sys
import os
import zipfile
import tempfile
import shutil
import json
from mspasspy.ccore.utility import  MsPASSError
import aws_settings

def updateZip(zip_path, filename):
    if not os.path.exists(zip_path):
        raise MsPASSError("Missing zip file: " + zip_path, "Fatal")
    if not os.path.exists(filename):
        raise MsPASSError("Missing file: " + filename, "Fatal")

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

    shutil.rmtree(tempdir)

def create_lambda_function(function_name):
    session = boto3.Session(aws_access_key_id = aws_settings.aws_access_key_id, aws_secret_access_key = aws_settings.aws_secret_access_key)
    
    # Create the archive
    updateZip('base.zip', 'aws_lambda_func_def.py')

    # Upload base.
    print('Uploading base.zip')
    s3_client = session.client('s3')
    s3_client.put_object(Key='base.zip', \
                        Bucket=aws_settings.lambda_upload_bucket, \
                        Body=open('base.zip', 'rb') \
                        )
    print('Upload complete')

    client = session.client('lambda')
    print('Creating lambda function')
    response = client.create_function(
        FunctionName=function_name,
        Runtime='python3.7',
        Role=aws_settings.lambda_iam_role,
        Handler='process.handler',
        Code={
            'S3Bucket': aws_settings.lambda_upload_bucket,
            'S3Key': 'base.zip'
        },
        Description='',
        Timeout=300,
        MemorySize=1024,
        Publish=True
    )
    print('Lambda function created')

def call_lambda_function(function_name, request):
    session = boto3.Session(aws_access_key_id = aws_settings.aws_access_key_id, aws_secret_access_key = aws_settings.aws_secret_access_key)
    client = session.client('lambda')
    response = client.invoke(
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
        raise MsPASSError("Undefined return type when calling " + function, "Fatal")
    
    ret_dict = {'return_type':ret_type, 'ret_value': ret_value}
    return ret_dict