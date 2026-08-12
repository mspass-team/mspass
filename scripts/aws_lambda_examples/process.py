# The API for aws lambda function.

import os
import boto3
import json
import base64
import tempfile
from aws_lambda_func_def import lambda_func

_DEBUG_FLAG_ = False  # When set to True, debugging log will be printed
MAX_PAYLOAD = 6000000  # Maximum final UTF-8 JSON response size for synchronous Invoke


def _serialized_response(response):
    return json.dumps(
        response,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def _fits_invoke_response(serialized_response):
    return len(serialized_response) <= MAX_PAYLOAD


def _remove_file(path):
    if path is None:
        return
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


def process(event):
    """
    A wrapper function for the user-defined aws lambda functions.
    First download the source object from s3 bucket.
    Then execute the lambda function defined by user.
    Finally, return the content when its compact UTF-8 JSON response is no
    larger than 6,000,000 bytes.  Otherwise, upload the output to S3 and return
    its key.
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

    session = boto3.Session()
    s3 = session.client("s3")
    suffix = os.path.splitext(os.path.basename(src_key))[1]
    input_path = None
    output_path = None
    try:
        input_handle = tempfile.NamedTemporaryFile(
            prefix="mspass-lambda-input-", suffix=suffix, delete=False
        )
        input_path = input_handle.name
        input_handle.close()
        if _DEBUG_FLAG_:
            print("Downloading {} to {}".format(src_key, input_path))
        s3.download_file(src_bucket, src_key, input_path)
        if not os.path.isfile(input_path):
            raise RuntimeError(
                "Could not download {} from {} to {}".format(
                    src_key, src_bucket, input_path
                )
            )

        candidate_path = lambda_func(input_path, event)
        if not isinstance(candidate_path, str) or not os.path.isfile(candidate_path):
            raise RuntimeError(f"Could not write output file {candidate_path!r}")
        output_path = candidate_path

        with open(output_path, "rb") as output_stream:
            output_bytes = output_stream.read()
        content_response = {
            "ret_type": "content",
            "ret_value": base64.b64encode(output_bytes).decode("utf-8"),
        }
        serialized_content = _serialized_response(content_response)
        if not _fits_invoke_response(serialized_content):
            save_to_s3 = True

        if save_to_s3:
            if not isinstance(dst_key, str) or not dst_key:
                raise RuntimeError("dst_key must be a nonempty S3 key")
            if _DEBUG_FLAG_:
                print("Saving {} to {}".format(output_path, dst_key))
            s3.upload_file(output_path, dst_bucket, dst_key)
            return {"ret_type": "key", "ret_value": dst_key}
        return content_response
    finally:
        try:
            _remove_file(output_path)
        finally:
            if output_path != input_path:
                _remove_file(input_path)


def handler(event, context):
    """
    Lambda function handler.
    """
    return process(event)
