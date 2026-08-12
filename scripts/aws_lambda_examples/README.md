This module utilizes the Amazon Lambda function to preprocess the data object stored in AWS S3. This is initially developed for the SCEDC dataset that is stored in S3, however could also be generalized and applied to any data object stored in S3.

The example targets the AWS Lambda Python 3.13 x86_64 runtime.  Its checked-in
`base.zip` is built in the AWS Python 3.13 image and contains Python 3.13 native
extensions; it must not be reused with another runtime or architecture.

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

**Rebuilding the deployment bundle:**

Docker with Buildx is required.  From the repository root, run:

```bash
scripts/aws_lambda_examples/build_base_zip.sh
```

The build uses the immutable AWS Lambda Python 3.13 image digest in
`Dockerfile.bundle`, installs every bundled dependency from the version-and-hash
lock in `bundle-requirements.txt`, imports every installed top-level module, and
performs a native ObsPy MiniSEED round trip before exporting `base.zip`.  The
AWS runtime supplies boto3/botocore, so they are deliberately not duplicated in
the archive.  The build is fixed to `linux/amd64`, matching Lambda's default
x86_64 architecture.

***Note:***
* The Lambda response protocol has two required forms: `ret_type="content"`
  carries base64 content in `ret_value`, while `ret_type="key"` carries the
  existing `bucket::key` S3 location in `ret_value`.  `call_lambda_function`
  continues to expose the public `return_type` and `ret_value` fields.
* Inline content is selected only when the final UTF-8 JSON response,
  including the base64 expansion and field syntax, is no larger than 6,000,000
  bytes.  Larger results are uploaded to the configured destination bucket.
* The maximum running time of one execution is 15 mins, so the lambda function can’t be used to do heavy work. The main point of  lambda function is to help do some trivial preprocessing on data on aws s3. If some heavy calculations are to be done, the better way is to download the data and do it locally.
