#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct 18 05:18:47 2026

@author: pavlis
"""
import os
from pathlib import Path

def datafile(dfname,filetype="yaml",verbose=False)->Path:
    # build search path
    searchpath=list()
    nextpath = Path(".").resolve()
    searchpath.append(nextpath)
    nextpath = nextpath / "data"
    nextpath = nextpath / filetype
    if "MSPASS_HOME" in os.environ:
        nextpath=Path(os.environ["MSPASS_HOME"]).resolve()
        searchpath.append(nextpath)
    nextpath = Path(__file__).resolve().parent.parent
    nextpath = nextpath / "data"
    nextpath = nextpath / filetype
    searchpath.append(nextpath)
    # now work though this list until we find a file that exists
    # and is writable
    for p in searchpath:
        f2test = p / dfname
        if verbose:
            print("Trying path=",f2test)
        if f2test.is_file() and os.access(f2test,os.R_OK):
            return f2test
    message = f'datafile:  could not find file {dfname}.  List of directories search:\n'
    for p in searchpath:
        message += f'{p}\n'
    raise FileNotFoundError(message)