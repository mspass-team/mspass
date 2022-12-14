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

import os
import subprocess
import sphinx_rtd_theme

# -- Project information -----------------------------------------------------

project = "MsPASS"
copyright = "2020-2021, Ian Wang"
author = "Ian Wang"

# The full version, including alpha/beta/rc tags
release = "0.0.1"


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx_rtd_theme",
    "sphinx.ext.githubpages",
    "sphinx.ext.autodoc",
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

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["that_style", "**/_build"]


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "sphinx_rtd_theme"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static", "doxygen"]

# If true, "Created using Sphinx" is shown in the HTML footer. Default is True.
html_show_sphinx = False

html_context = {
    "display_github": True,
    "github_user": "mspass-team",
    "github_repo": "mspass",
    "github_version": "master",
    "conf_py_path": "/docs/source/",
    "theme_vcs_pageview_mode": "blob",
}

# Breathe Configuration
breathe_default_project = "MsPASS C++ API"
breathe_projects = {}
subprocess.call("doxygen Doxyfile", shell=True)
breathe_projects["MsPASS C++ API"] = "./doxygen/xml"

# Enable figure numbering
numfig = True

# Create csv files for schema
# FIXME the following script needs a rewrite
mspass_home = os.path.abspath("../..")
subprocess.call(
    "cd mspass_schema; MSPASS_HOME=" + mspass_home + " python3 build_metadata_tbls.py",
    shell=True,
)
