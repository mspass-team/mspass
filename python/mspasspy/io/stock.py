#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Locate packaged or user-supplied MsPASS data files."""

import os
from pathlib import Path


def datafile(dfname, filetype="yaml", verbose=False) -> Path:
    """Find a readable file in the working directory or MsPASS data locations."""
    cwd = Path.cwd()
    searchpath = [cwd, cwd / "data" / filetype]
    if "MSPASS_HOME" in os.environ:
        home = Path(os.environ["MSPASS_HOME"]).expanduser().resolve()
        searchpath.extend([home, home / "data" / filetype])
    searchpath.append(Path(__file__).resolve().parent.parent / "data" / filetype)

    for directory in searchpath:
        candidate = directory / dfname
        if verbose:
            print("Trying path=", candidate)
        if candidate.is_file() and os.access(candidate, os.R_OK):
            return candidate

    searched = "\n".join(str(directory) for directory in searchpath)
    raise FileNotFoundError(
        f"datafile: could not find file {dfname}. Searched:\n{searched}"
    )
