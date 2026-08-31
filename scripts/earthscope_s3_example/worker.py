"""Worker-local EarthScope S3 client support for the example workflow."""

from datetime import datetime, timedelta, timezone

from boto3 import Session
from botocore.config import Config
from botocore.credentials import RefreshableCredentials
from botocore.session import get_session
from dask.distributed import WorkerPlugin, get_worker

S3_CONFIG = Config(
    request_checksum_calculation="when_required",
    response_checksum_validation="when_required",
)


def fetch_s3_client(session=None, worker_data_key="earthscope_s3_client"):
    """Return a serial client or the client installed on the current worker."""
    if session is not None:
        return session.client("s3", config=S3_CONFIG)

    try:
        worker = get_worker()
    except Exception as error:
        raise ValueError(
            "fetch_s3_client requires a boto3 session outside a Dask worker"
        ) from error

    try:
        return worker.data[worker_data_key]
    except KeyError as error:
        raise ValueError(
            f"worker has no S3 client under key {worker_data_key!r}; "
            "register EarthScopeS3Worker before submitting the workflow"
        ) from error


def _fresh_earthscope_credentials():
    """Fetch short-lived credentials at runtime without serializing them."""
    from earthscope_sdk import EarthScopeClient

    session = EarthScopeClient().user.get_boto3_session()
    credentials = session.get_credentials()
    frozen = credentials.get_frozen_credentials()
    expiry = getattr(credentials, "_expiry_time", None)
    if isinstance(expiry, datetime):
        expiry_string = expiry.astimezone(timezone.utc).isoformat()
    elif isinstance(expiry, str):
        expiry_string = expiry
    else:
        expiry_string = (datetime.now(timezone.utc) + timedelta(minutes=55)).isoformat()
    return {
        "access_key": frozen.access_key,
        "secret_key": frozen.secret_key,
        "token": frozen.token,
        "expiry_time": expiry_string,
    }


def create_earthscope_s3_client():
    """Create a worker client whose EarthScope credentials refresh in place."""
    credentials = RefreshableCredentials.create_from_metadata(
        metadata=_fresh_earthscope_credentials(),
        refresh_using=_fresh_earthscope_credentials,
        method="sts-assume-role",
    )
    botocore_session = get_session()
    botocore_session._credentials = credentials
    return Session(botocore_session=botocore_session).client("s3", config=S3_CONFIG)


class EarthScopeS3Worker(WorkerPlugin):
    """Install one refreshable S3 client in each Dask worker."""

    def __init__(self, key="earthscope_s3_client"):
        self.key = key

    def setup(self, worker):
        worker.data[self.key] = create_earthscope_s3_client()

    def teardown(self, worker):
        client = worker.data.pop(self.key, None)
        if client is not None:
            client.close()
