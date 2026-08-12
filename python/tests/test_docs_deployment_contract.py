import importlib.util
import io
import json
import os
import shutil
import subprocess
import tarfile
from pathlib import Path

import pytest
import yaml

REPOSITORY_ROOT = Path(
    os.environ.get("MSPASS_TEST_REPOSITORY_ROOT", Path(__file__).resolve().parents[2])
)
PREPARE_SCRIPT = REPOSITORY_ROOT / "docs" / "scripts" / "prepare_versioned_docs.py"


def _load_prepare_module():
    spec = importlib.util.spec_from_file_location(
        "prepare_versioned_docs", PREPARE_SCRIPT
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _run(*args, cwd):
    subprocess.run(args, cwd=cwd, check=True, capture_output=True, text=True)


def _write_tree(root, files):
    for name, contents in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents, encoding="utf-8")


def _seed_pages_repository(tmp_path):
    remote = tmp_path / "pages.git"
    publisher = tmp_path / "publisher"
    source = tmp_path / "source"
    _run("git", "init", "--bare", str(remote), cwd=tmp_path)
    _run("git", "clone", str(remote), str(publisher), cwd=tmp_path)
    _run("git", "config", "user.name", "MsPASS test", cwd=publisher)
    _run("git", "config", "user.email", "mspass@example.invalid", cwd=publisher)
    _run("git", "switch", "--orphan", "gh-pages", cwd=publisher)
    _write_tree(
        publisher,
        {
            "CNAME": "www.mspass.org\n",
            "index.html": "old latest\n",
            "stale-root.html": "remove from latest\n",
            "latest/index.html": "old latest\n",
            "latest/stale-latest.html": "remove from latest\n",
            "v2.4.0/index.html": "old v2.4.0\n",
            "v2.4.0/stale-version.html": "remove from v2.4.0\n",
            "v2.3.0/keep.html": "preserve unrelated version\n",
        },
    )
    _run("git", "add", ".", cwd=publisher)
    _run("git", "commit", "-m", "seed pages", cwd=publisher)
    _run("git", "push", "-u", "origin", "gh-pages", cwd=publisher)

    _run("git", "clone", str(remote), str(source), cwd=tmp_path)
    return publisher, source


def _deploy_site(publisher, site_dir, message):
    for item in publisher.iterdir():
        if item.name == ".git":
            continue
        if item.is_dir():
            shutil.rmtree(item)
        else:
            item.unlink()
    for item in site_dir.iterdir():
        destination = publisher / item.name
        if item.is_dir():
            shutil.copytree(item, destination)
        else:
            shutil.copy2(item, destination)
    _run("git", "add", "-A", cwd=publisher)
    _run("git", "commit", "-m", message, cwd=publisher)
    _run("git", "push", "origin", "gh-pages", cwd=publisher)


def _prepare_and_deploy(module, source, publisher, tmp_path, target):
    html_dir = tmp_path / f"html-{target}"
    site_dir = tmp_path / f"site-{target}"
    html_dir.mkdir()
    _write_tree(
        html_dir,
        {
            "CNAME": "rebuilt docs must not replace site control\n",
            "index.html": f"new {target}\n",
            "new-page.html": f"new page for {target}\n",
        },
    )
    previous_cwd = Path.cwd()
    try:
        os.chdir(source)
        module.prepare_docs(
            html_dir,
            site_dir,
            "https://www.mspass.org",
            target,
        )
    finally:
        os.chdir(previous_cwd)
    _deploy_site(publisher, site_dir, f"deploy {target}")


@pytest.mark.parametrize("order", [("latest", "v2.4.0"), ("v2.4.0", "latest")])
def test_normal_and_backfill_deployments_replace_only_the_target(tmp_path, order):
    module = _load_prepare_module()
    publisher, source = _seed_pages_repository(tmp_path)

    for index, target in enumerate(order):
        _prepare_and_deploy(module, source, publisher, tmp_path, target)
        assert (publisher / "CNAME").read_text(encoding="utf-8") == "www.mspass.org\n"
        assert (publisher / "v2.3.0" / "keep.html").is_file()
        if target == "latest":
            assert not (publisher / "stale-root.html").exists()
            assert not (publisher / "latest" / "stale-latest.html").exists()
            assert (publisher / "index.html").read_text(
                encoding="utf-8"
            ) == "new latest\n"
            assert (publisher / "latest" / "index.html").read_text(
                encoding="utf-8"
            ) == "new latest\n"
            if index == 0:
                assert (publisher / "v2.4.0" / "stale-version.html").is_file()
        else:
            assert not (publisher / "v2.4.0" / "stale-version.html").exists()
            assert (publisher / "v2.4.0" / "index.html").read_text(
                encoding="utf-8"
            ) == "new v2.4.0\n"
            if index == 0:
                assert (publisher / "stale-root.html").is_file()
                assert (publisher / "latest" / "stale-latest.html").is_file()

    assert not (publisher / "stale-root.html").exists()
    assert not (publisher / "latest" / "stale-latest.html").exists()
    assert not (publisher / "v2.4.0" / "stale-version.html").exists()
    assert (publisher / "v2.3.0" / "keep.html").is_file()
    switcher = json.loads((publisher / "switcher.json").read_text(encoding="utf-8"))
    assert [entry["version"] for entry in switcher] == [
        "latest",
        "v2.4.0",
        "v2.3.0",
    ]
    assert "preferred" not in switcher[0]
    assert switcher[1]["preferred"] is True


@pytest.mark.parametrize("target", ["latest", "v1.0.0"])
def test_first_deployment_ignores_a_stale_tracking_ref(tmp_path, target):
    module = _load_prepare_module()
    remote = tmp_path / "empty-pages.git"
    source = tmp_path / "source"
    _run("git", "init", "--bare", str(remote), cwd=tmp_path)
    _run("git", "init", str(source), cwd=tmp_path)
    _run("git", "config", "user.name", "MsPASS test", cwd=source)
    _run("git", "config", "user.email", "mspass@example.invalid", cwd=source)
    _run("git", "remote", "add", "origin", str(remote), cwd=source)
    _write_tree(source, {"stale.html": "must not be restored\n"})
    _run("git", "add", "stale.html", cwd=source)
    _run("git", "commit", "-m", "stale local pages", cwd=source)
    _run(
        "git",
        "update-ref",
        "refs/remotes/origin/gh-pages",
        "HEAD",
        cwd=source,
    )

    html_dir = tmp_path / "html"
    site_dir = tmp_path / "site"
    _write_tree(html_dir, {"index.html": f"new {target}\n"})
    previous_cwd = Path.cwd()
    try:
        os.chdir(source)
        module.prepare_docs(html_dir, site_dir, "https://www.mspass.org", target)
    finally:
        os.chdir(previous_cwd)

    assert not (site_dir / "stale.html").exists()
    assert (site_dir / ".nojekyll").is_file()
    if target == "latest":
        assert (site_dir / "index.html").is_file()
        assert (site_dir / "latest" / "index.html").is_file()
    else:
        assert (site_dir / target / "index.html").is_file()
        assert not (site_dir / "index.html").exists()
    switcher = json.loads((site_dir / "switcher.json").read_text(encoding="utf-8"))
    assert [entry["version"] for entry in switcher] == (
        ["latest"] if target == "latest" else ["latest", target]
    )


def test_deployment_from_an_existing_empty_pages_branch(tmp_path):
    module = _load_prepare_module()
    remote = tmp_path / "pages.git"
    publisher = tmp_path / "publisher"
    source = tmp_path / "source"
    _run("git", "init", "--bare", str(remote), cwd=tmp_path)
    _run("git", "clone", str(remote), str(publisher), cwd=tmp_path)
    _run("git", "config", "user.name", "MsPASS test", cwd=publisher)
    _run("git", "config", "user.email", "mspass@example.invalid", cwd=publisher)
    _run("git", "switch", "--orphan", "gh-pages", cwd=publisher)
    _run("git", "commit", "--allow-empty", "-m", "empty pages", cwd=publisher)
    _run("git", "push", "-u", "origin", "gh-pages", cwd=publisher)
    _run("git", "clone", str(remote), str(source), cwd=tmp_path)
    html_dir = tmp_path / "html"
    site_dir = tmp_path / "site"
    _write_tree(html_dir, {"index.html": "first page\n"})

    previous_cwd = Path.cwd()
    try:
        os.chdir(source)
        module.prepare_docs(html_dir, site_dir, "https://www.mspass.org", "latest")
    finally:
        os.chdir(previous_cwd)

    assert (site_dir / "index.html").read_text(encoding="utf-8") == "first page\n"
    assert (site_dir / "latest" / "index.html").is_file()
    assert json.loads((site_dir / "switcher.json").read_text(encoding="utf-8")) == [
        {
            "name": "latest",
            "version": "latest",
            "url": "https://www.mspass.org/",
        }
    ]


def test_fetch_failure_does_not_fall_back_to_a_stale_tracking_ref(tmp_path):
    module = _load_prepare_module()
    _, source = _seed_pages_repository(tmp_path)
    _run(
        "git", "remote", "set-url", "origin", str(tmp_path / "missing.git"), cwd=source
    )
    html_dir = tmp_path / "html"
    site_dir = tmp_path / "site"
    _write_tree(html_dir, {"index.html": "new latest\n"})

    previous_cwd = Path.cwd()
    try:
        os.chdir(source)
        with pytest.raises(subprocess.CalledProcessError):
            module.prepare_docs(html_dir, site_dir, "https://www.mspass.org", "latest")
    finally:
        os.chdir(previous_cwd)
    assert not site_dir.exists()


def _archive_with_member(name, member_type):
    payload = io.BytesIO()
    with tarfile.open(fileobj=payload, mode="w") as archive:
        member = tarfile.TarInfo(name)
        member.type = member_type
        if member_type == tarfile.REGTYPE:
            contents = b"unsafe\n"
            member.size = len(contents)
            archive.addfile(member, io.BytesIO(contents))
        else:
            if member_type == tarfile.SYMTYPE:
                member.linkname = "target"
            archive.addfile(member)
    return payload.getvalue()


@pytest.mark.parametrize(
    ("name", "member_type", "message"),
    [
        ("../escaped", tarfile.REGTYPE, "Unsafe path"),
        ("link", tarfile.SYMTYPE, "Unsupported entry"),
        ("fifo", tarfile.FIFOTYPE, "Unsupported entry"),
    ],
)
def test_deployed_site_archive_rejects_unsafe_entries(
    tmp_path, monkeypatch, name, member_type, message
):
    module = _load_prepare_module()
    payload = _archive_with_member(name, member_type)
    monkeypatch.setattr(module, "_fetch_deployed_site", lambda: True)
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args[0], 0, stdout=payload, stderr=b""
        ),
    )

    site_dir = tmp_path / "site"
    with pytest.raises(ValueError, match=message):
        module._restore_deployed_site(site_dir)
    assert not (tmp_path / "escaped").exists()
    assert not (site_dir / name).exists()


def test_every_pages_writer_uses_the_shared_queued_lock_and_clean_deploy():
    normal_workflow = yaml.safe_load(
        (REPOSITORY_ROOT / ".github" / "workflows" / "python-package.yml").read_text(
            encoding="utf-8"
        )
    )
    backfill_workflow = yaml.safe_load(
        (
            REPOSITORY_ROOT / ".github" / "workflows" / "documentation-backfill.yml"
        ).read_text(encoding="utf-8")
    )
    publisher_workflow = yaml.safe_load(
        (
            REPOSITORY_ROOT / ".github" / "workflows" / "publish-documentation.yml"
        ).read_text(encoding="utf-8")
    )
    normal = normal_workflow["jobs"]["build"]
    backfill = backfill_workflow["jobs"]["publish-docs"]
    publisher = publisher_workflow["jobs"]["publish-docs"]

    assert backfill_workflow["concurrency"] == {
        "group": "gh-pages-writer",
        "queue": "max",
        "cancel-in-progress": False,
    }
    assert publisher_workflow["concurrency"] == backfill_workflow["concurrency"]
    assert "workflow_call" in publisher_workflow["on"]

    pages_writers = {}
    for workflow_path in (REPOSITORY_ROOT / ".github" / "workflows").glob("*.yml"):
        workflow = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))
        for job_name, job in workflow.get("jobs", {}).items():
            for step in job.get("steps", []):
                options = step.get("with", {})
                if (
                    step.get("uses") == "JamesIves/github-pages-deploy-action@3.7.1"
                    and isinstance(options, dict)
                    and options.get("BRANCH") == "gh-pages"
                ):
                    pages_writers[(workflow_path.name, job_name)] = workflow.get(
                        "concurrency"
                    )
    assert pages_writers == {
        ("documentation-backfill.yml", "publish-docs"): {
            "group": "gh-pages-writer",
            "queue": "max",
            "cancel-in-progress": False,
        },
        ("publish-documentation.yml", "publish-docs"): {
            "group": "gh-pages-writer",
            "queue": "max",
            "cancel-in-progress": False,
        },
    }
    assert normal_workflow["concurrency"]["group"] == (
        "${{ github.event_name == 'pull_request' && "
        "format('{0}-{1}', github.workflow, github.ref) || github.run_id }}"
    )
    assert normal_workflow["concurrency"]["cancel-in-progress"] == (
        "${{ github.event_name == 'pull_request' }}"
    )
    assert "queue" not in normal_workflow["concurrency"]

    upload = next(
        step
        for step in normal["steps"]
        if step.get("name") == "Upload documentation for publication"
    )
    assert upload["uses"] == "actions/upload-artifact@v4"
    assert upload["if"] == (
        "${{ (github.ref == 'refs/heads/master' || "
        "startsWith(github.ref, 'refs/tags/v')) && "
        "matrix.python-version == '3.10' }}"
    )
    assert upload["with"] == {
        "name": "documentation-html",
        "path": "docs/build/html",
        "include-hidden-files": True,
    }
    assert not any(
        step.get("name") == "Deploy to GitHub Pages" for step in normal["steps"]
    )
    publish_call = normal_workflow["jobs"]["publish-docs"]
    assert publish_call["if"] == (
        "${{ github.ref == 'refs/heads/master' || "
        "startsWith(github.ref, 'refs/tags/v') }}"
    )
    assert publish_call["needs"] == "build"
    assert publish_call["uses"] == "./.github/workflows/publish-documentation.yml"
    assert publish_call["permissions"] == {"contents": "write"}
    assert publish_call["with"]["version_match"] == (
        "${{ startsWith(github.ref, 'refs/tags/v') && " "github.ref_name || 'latest' }}"
    )

    download = next(
        step
        for step in publisher["steps"]
        if step.get("name") == "Download documentation"
    )
    assert download["uses"] == "actions/download-artifact@v4"
    assert download["with"] == {
        "name": "documentation-html",
        "path": "docs/build/html",
    }

    for job in (backfill, publisher):
        assert "concurrency" not in job
        prepare = next(
            step
            for step in job["steps"]
            if step.get("name") == "Prepare Documentation for GitHub Pages"
        )
        assert "prepare_versioned_docs.py" in prepare["run"]
        assert "|| true" not in prepare["run"]
        deploy = next(
            step
            for step in job["steps"]
            if step.get("name") == "Deploy to GitHub Pages"
        )
        assert deploy["with"]["BRANCH"] == "gh-pages"
        assert deploy["with"]["CLEAN"] is True
