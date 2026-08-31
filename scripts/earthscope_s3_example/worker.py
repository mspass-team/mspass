"""Worker-local EarthScope S3 client support for the example workflow."""

from botocore.config import Config
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


def create_earthscope_s3_client(role="s3-miniseed-v2"):
    """Create an S3 client from the SDK's refreshable boto3 session."""
    from earthscope_sdk import EarthScopeClient

    session = EarthScopeClient().user.get_boto3_session(role=role)
    return session.client("s3", config=S3_CONFIG)


class EarthScopeS3Worker(WorkerPlugin):
    """Install one refreshable S3 client in each Dask worker."""

    def __init__(self, key="earthscope_s3_client", role="s3-miniseed-v2"):
        self.key = key
        self.role = role

    def setup(self, worker):
        worker.data[self.key] = create_earthscope_s3_client(role=self.role)

    def teardown(self, worker):
        client = worker.data.pop(self.key, None)
        if client is not None:
            client.close()
