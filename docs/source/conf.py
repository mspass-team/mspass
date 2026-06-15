# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
# import os
# import sys
# sys.path.insert(0, os.path.abspath('.'))

import importlib
import importlib.machinery
import os
import shutil
import subprocess
import sys
import types
from datetime import date
from importlib.metadata import PackageNotFoundError, version as package_version


conf_dir = os.path.abspath(os.path.dirname(__file__))
repo_root = os.path.abspath(os.path.join(conf_dir, "../.."))
sys.path.insert(0, os.path.join(repo_root, "python"))


def _bridge_installed_ccore_if_needed():
    """Use real installed ccore extensions when source-tree extensions do not load.

    Local checkouts can contain stale extension modules built for another Python
    ABI.  The Python API docs should still import current Python source files,
    but ccore imports must resolve to real extension modules rather than mocks.
    """
    try:
        import mspasspy  # noqa: F401

        importlib.import_module("mspasspy.ccore.utility")
        return
    except (ImportError, OSError) as error:
        ccore_import_error = error
        for name in list(sys.modules):
            if name == "mspasspy.ccore" or name.startswith("mspasspy.ccore."):
                del sys.modules[name]

    ccore_dir = None
    extension_suffixes = importlib.machinery.EXTENSION_SUFFIXES
    searched_dirs = []
    for path in sys.path[1:]:
        candidate_dir = os.path.join(path, "mspasspy", "ccore")
        searched_dirs.append(candidate_dir)
        if any(
            os.path.exists(os.path.join(candidate_dir, "utility" + suffix))
            for suffix in extension_suffixes
        ):
            ccore_dir = candidate_dir
            break
    if ccore_dir is None:
        searched = ", ".join(searched_dirs) or "<empty sys.path>"
        raise RuntimeError(
            "Could not import an ABI-compatible mspasspy.ccore.utility "
            "extension for API docs. Build the C++ extension modules or "
            "install mspasspy in the active environment before running "
            f"Sphinx. Original import error: {ccore_import_error!r}. "
            f"Searched: {searched}"
        )

    import mspasspy

    ccore_pkg = types.ModuleType("mspasspy.ccore")
    ccore_pkg.__path__ = [ccore_dir]
    ccore_pkg.__package__ = "mspasspy.ccore"
    ccore_pkg.__file__ = os.path.join(ccore_dir, "__init__.py")
    sys.modules["mspasspy.ccore"] = ccore_pkg
    mspasspy.ccore = ccore_pkg

    algorithms_pkg = types.ModuleType("mspasspy.ccore.algorithms")
    algorithms_pkg.__path__ = [os.path.join(ccore_dir, "algorithms")]
    algorithms_pkg.__package__ = "mspasspy.ccore.algorithms"
    algorithms_pkg.__file__ = os.path.join(ccore_dir, "algorithms", "__init__.py")
    sys.modules["mspasspy.ccore.algorithms"] = algorithms_pkg
    ccore_pkg.algorithms = algorithms_pkg


_bridge_installed_ccore_if_needed()

# -- Project information -----------------------------------------------------

project = "MsPASS"
copyright = f"2020-{date.today().year}, MsPASS Team"
author = "MsPASS Team"


def _get_project_version():
    """Resolve the docs version from the same Git metadata used for packaging."""
    fallback_version = "0+unknown"
    try:
        from setuptools_scm import get_version

        scm_version = get_version(root=repo_root, fallback_version=fallback_version)
        if scm_version != fallback_version:
            return scm_version
    except Exception:
        pass

    try:
        return package_version("mspasspy")
    except PackageNotFoundError:
        return fallback_version


# The full version, including alpha/beta/rc tags
release = _get_project_version()
version = release.split("+", 1)[0]


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.githubpages",
    "sphinx.ext.autodoc",
    "sphinx.ext.mathjax",
    "m2r2",
    "breathe",
    "sphinx.ext.todo",
    "sphinx.ext.viewcode",
    "sphinx_copybutton",
    "sphinx.ext.intersphinx",
]

intersphinx_mapping = {
    "obspy": ("https://docs.obspy.org/", None),
    "pymongo": ("https://pymongo.readthedocs.io/en/stable", None),
}

# Project templates are limited to small theme components.  The main layout is
# still provided by pydata-sphinx-theme.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = [
    "that_style",
    "**/_build",
    "getting_started/deploy_mspass_with_docker_compose.rst",
    "getting_started/quick_start.rst",
    "getting_started/run_mspass_with_docker.rst",
    "user_manual/appendix_historical_perspective_essay.rst",
    "user_manual/research_computing_essay.rst",
]


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "pydata_sphinx_theme"

html_theme_options = {
    "logo": {"text": "MsPASS"},
    "github_url": "https://github.com/mspass-team/mspass",
    "use_edit_page_button": True,
    "show_toc_level": 2,
    "show_nav_level": 2,
    "navigation_depth": 4,
    "collapse_navigation": False,
    "navbar_align": "content",
    "navbar_center": ["navbar-sections.html"],
    "navbar_persistent": ["search-button-field"],
    "search_bar_text": "Search MsPASS docs...",
    "primary_sidebar_end": [],
    "footer_end": [],
}

html_sidebars = {
    "**": ["sidebar-nav-full.html"],
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static", "doxygen"]
html_css_files = ["css/mspass.css"]

# If true, "Created using Sphinx" is shown in the HTML footer. Default is True.
html_show_sphinx = False

html_context = {
    "github_user": "mspass-team",
    "github_repo": "mspass",
    "github_version": "master",
    "doc_path": "docs/source",
}

# Breathe Configuration
breathe_default_project = "MsPASS C++ API"
breathe_projects = {}
breathe_projects["MsPASS C++ API"] = "./doxygen/xml"

# Enable figure numbering
numfig = True

def _generate_doxygen_xml(app):
    """Generate Doxygen XML before Breathe reads the C++ API sources."""
    for command, package in (("doxygen", "Doxygen"), ("dot", "Graphviz")):
        if shutil.which(command) is None:
            raise RuntimeError(
                f"{package} command '{command}' is required to build the "
                "MsPASS C++ API docs. Install it before running Sphinx."
            )

    subprocess.run(
        ["doxygen", "Doxyfile"],
        cwd=conf_dir,
        env={**os.environ, "MSPASS_DOC_VERSION": release},
        check=True,
    )


def _generate_schema_tables(app):
    """Generate schema CSV tables before Sphinx reads the schema page."""
    subprocess.run(
        [sys.executable, "build_metadata_tbls.py"],
        cwd=os.path.join(conf_dir, "mspass_schema"),
        env={**os.environ, "MSPASS_HOME": repo_root},
        check=True,
    )


def setup(app):
    # Breathe is the bridge between Doxygen's C++ XML and Sphinx pages.  Running
    # generation at builder startup keeps imports of this config side-effect
    # light while ensuring generated inputs exist before source files are read.
    app.connect("builder-inited", _generate_doxygen_xml)
    app.connect("builder-inited", _generate_schema_tables)
