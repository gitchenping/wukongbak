#encoding=utf-8
import sys,time
import requests
import json
import sys
import os
import time
from utils.util import cmd_linux


def test(a):

    if a=='1':
        x=test(2)
        y=test(3)
        return x['a']+y['a']
    return {'a':a*2,'b':a*3}

print(test('1'))