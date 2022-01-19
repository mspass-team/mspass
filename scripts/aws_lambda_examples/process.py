# The API for aws lambda function.

import os
import boto3
import obspy
import json
import base64
import shutil
from aws_lambda_func_def import lambda_func


_DEBUG_FLAG_ = False  # When set to True, debugging log will be printed
MAX_PAYLOAD = 6 * 1000000  # The maximum payload of lambda function is 6MB


def process(event):
    """
    A wrapper function for the user-defined aws lambda functions.
    First download the source object from s3 bucket.
    Then execute the lambda function defined by user.
    Finally, return the content of the timewindow if the file size is less than 6MB,
    Otherwise, dump that output file to another s3 bucket, and return the file location.
    """
    if _DEBUG_FLAG_:
        print(event)

    if "save_to_s3" in event:
        save_to_s3 = event["save_to_s3"]
    else:
        save_to_s3 = False

    #   Variable initialization
    src_bucket = event["src_bucket"]
    dst_bucket = event["dst_bucket"]
    src_key = event["src_key"]
    dst_key = event["dst_key"]

    # Download file from the source S3 bucket.
    session = boto3.Session()
    s3 = boto3.client("s3", region_name="us-west-2")  #   BY DEFAULT
    tempfile = "/tmp/" + os.path.basename(src_key)
    if _DEBUG_FLAG_:
        print("Downloading {} to {}".format(src_key, tempfile))
    s3.download_file(src_bucket, src_key, tempfile)

    if not os.path.isfile(tempfile):
        raise Exception(
            "Could not download {} from {} to {}".format(src_key, src_bucket, tempfile)
        )

    # Call user-defined function
    outfile = lambda_func(tempfile, event)
    if not os.path.isfile(outfile):
        raise Exception("Could not write output file {}".format(outfile))

    os.remove(tempfile)

    # Check the size of output file
    output_size = os.path.getsize(outfile)
    if _DEBUG_FLAG_:
        print("The size of {} is {}".format(outfile, output_size / 1000000.0))
    if output_size > MAX_PAYLOAD:  #   We have to save that to another s3 bucket
        save_to_s3 = True

    # Save output to the output s3 bucket
    if save_to_s3:
        if _DEBUG_FLAG_:
            print("Saving {} to {}".format(outfile, dst_key))
        s3.upload_file(outfile, dst_bucket, dst_key)

    ret_dict = {}
    if save_to_s3:
        ret_dict["ret_type"] = "key"
        ret_dict["ret_value"] = dst_bucket + "::" + dst_key
    else:
        ret_dict["ret_type"] = "content"
        outfile_bytes = open(outfile, "rb").read()
        ret_dict["ret_value"] = base64.b64encode(outfile_bytes).decode("utf-8")

    os.remove(outfile)

    return ret_dict


def handler(event, context):
    """
    Lambda function handler.
    """
    return process(event)
