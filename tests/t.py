# -*- coding: utf-8 -
# Copyright 2009 Paul J. Davis <paul.joseph.davis@gmail.com>
#
# This file is part of gunicorn released under the MIT license. 
# See the NOTICE for more information.

import os
import tempfile

dirname = os.path.dirname(__file__)

from gunicorn.http.parser import HttpParser
from gunicorn.http.request import HttpRequest

def data_source(fname):
    with open(fname) as handle:
        lines = []
        for line in handle:
            line = line.rstrip("\n").replace("\\r\\n", "\r\n")
            lines.append(line)
        return "".join(lines)

class request(object):
    def __init__(self, name):
        self.fname = os.path.join(dirname, "requests", name)
        
    def __call__(self, func):
        def run():
            src = data_source(self.fname)
            func(src, HttpParser())
        run.func_name = func.func_name
        return run
        
        
class FakeSocket(object):
    
    def __init__(self, data=""):
        self.tmp = tempfile.TemporaryFile()
        if data:
            self.tmp.write(data)
            self.tmp.flush()
            self.tmp.seek(0)

    def fileno(self):
        return self.tmp.fileno()
        
    def len(self):
        return self.tmp.len
        
    def recv(self, length=None):
        return self.tmp.read()
        
    def send(self, data):
        self.tmp.write(data)
        self.tmp.flush()
        
    def seek(self, offset, whence=0):
        self.tmp.seek(offset, whence)
        
        
class http_request(object):
    def __init__(self, name):
        self.fname = os.path.join(dirname, "requests", name)
    
    def __call__(self, func):
        def run():
            fsock = FakeSocket(data_source(self.fname))
            req = HttpRequest(fsock, ('127.0.0.1', 6000), 
                            ('127.0.0.1', 8000))
            func(req)
        run.func_name = func.func_name
        return run
    
        
        
        
    
def eq(a, b):
    assert a == b, "%r != %r" % (a, b)

def ne(a, b):
    assert a != b, "%r == %r" % (a, b)

def lt(a, b):
    assert a < b, "%r >= %r" % (a, b)

def gt(a, b):
    assert a > b, "%r <= %r" % (a, b)

def isin(a, b):
    assert a in b, "%r is not in %r" % (a, b)

def isnotin(a, b):
    assert a not in b, "%r is in %r" % (a, b)

def has(a, b):
    assert hasattr(a, b), "%r has no attribute %r" % (a, b)

def hasnot(a, b):
    assert not hasattr(a, b), "%r has an attribute %r" % (a, b)

def raises(exctype, func, *args, **kwargs):
    try:
        func(*args, **kwargs)
    except exctype, inst:
        pass
    else:
        func_name = getattr(func, "func_name", "<builtin_function>")
        raise AssertionError("Function %s did not raise %s" % (
            func_name, exctype.__name__))

