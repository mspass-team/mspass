import os
import subprocess
import sys


def _run_with_broken_pyspark(tmp_path, code):
    pyspark_package = tmp_path / "pyspark"
    pyspark_package.mkdir()
    (pyspark_package / "__init__.py").write_text(
        'raise TypeError("broken pyspark import")\n', encoding="utf-8"
    )

    env = os.environ.copy()
    pythonpath = env.get("PYTHONPATH")
    env["PYTHONPATH"] = str(tmp_path)
    if pythonpath:
        env["PYTHONPATH"] += os.pathsep + pythonpath

    return subprocess.run(
        [sys.executable, "-c", code],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def test_client_import_survives_broken_optional_pyspark(tmp_path):
    result = _run_with_broken_pyspark(
        tmp_path,
        "from mspasspy.client import Client",
    )

    assert result.returncode == 0, result.stderr


def test_explicit_spark_reports_broken_optional_pyspark(tmp_path):
    result = _run_with_broken_pyspark(
        tmp_path,
        """
from mspasspy.client import Client
from mspasspy.ccore.utility import MsPASSError

try:
    Client(scheduler="spark")
except MsPASSError as err:
    message = str(err)
    assert "PySpark could not be imported" in message, message
    assert "broken pyspark import" in message, message
else:
    raise AssertionError("Client(scheduler='spark') should fail when PySpark is broken")
""",
    )

    assert result.returncode == 0, result.stderr
