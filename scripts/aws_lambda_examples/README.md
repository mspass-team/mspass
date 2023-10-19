This module utilizes the Amazon Lambda function to preprocess the data object stored in AWS S3. This is initially developed for the SCEDC dataset that is stored in S3, however could also be generalized and applied to any data object stored in S3. 

**AWS Lambda:**

AWS lambda is a serverless service that allows users to deploy applications or services on cloud. It has two advantages:
Lambda  functions are deployed on AWS machines so it can access S3 data faster. 
It allows users to process data without downloading it.

**Workflow:**

This module includes two main functions: _create_lambda_function_ and _call_lambda_function_. 
_create_lambda_function_ uploads a user_defined lambda function to the AWS. _call_lambda_function_ calls an existing lambda function. More information can be found in the comments sections.

Here is a typical workflow of using lambda functions:
1. Write a python script containing the lambda function that you would like to upload, the example is given in the aws_lambda_func_def.py
2. Create the lambda function using ‘create_lambda_function’. The lambda function will be packed into an archive including the running environment and uploaded to AWS.
3. Call the new lambda function using ‘call_lambda_function’ to operate on s3 files and download the output object.
4. Process the object locally (indexing data, apply algorithms)

**Prerequisites:**

To use lambda functions, users should have their own AWS account. And Create an IAM role that has full AmazonS3FullAccess, AWSLambda_FullAccess, and AWSLambdaBasicExecutionRole permissions. For more information and instruction, check out https://console.aws.amazon.com/iam/home#/roles. The account information will be used when creating and calling lambda functions.

***Note:***
* Because the size of response payload is limited, we can have two different API: 
    * When the size of the object to return is small enough (Less than 5~6 MB), it can be dumped into the payload. In such a case, the return value of the call_lambda_function is a data object, which is directly obtained from the lambda function call.
    * When the size of the object to return is larger than 5~6MB, it can’t be transmitted through the lambda payload. So we can just return the location of the output object, and let the user download it later. 
* The maximum running time of one execution is 15 mins, so the lambda function can’t be used to do heavy work. The main point of  lambda function is to help do some trivial preprocessing on data on aws s3. If some heavy calculations are to be done, the better way is to download the data and do it locally.
