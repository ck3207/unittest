# -*- coding: utf-8 -*-
import json

import requests
__author__ = "chenk"

class Interfaces:
    """接口请求类"""

    URL_PREFIX = "http://10.20.18.174:8084/fc/"
    HEADERS = {"Content-Type": "application/json"}

    def __init__(self):
        pass

    def request(self, url, data, is_get=True, verify=False, headers={}):
        print("Will Reuqeuest Interface: {}".format(url))
        print("Augues as follows: \n{}".format(data))
        if not headers:
            headers = Interfaces.HEADERS
        if is_get:
            r = requests.get(url=url, headers=headers, verify=verify, params=data)
        else:
            r = requests.post(url=url, headers=headers, verify=verify, data=data)
        result = r.json()
        print("Response: \n{}".format(result))
        return result

    def get_url_prefix(self):
        return Interfaces.URL_PREFIX


interfaces = Interfaces()