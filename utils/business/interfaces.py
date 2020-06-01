# -*- coding: utf-8 -*-
import json

import requests
__author__ = "chenk"

class Interfaces:
    """接口请求类"""

    ANALYSIS_FC_PREFIX = "http://10.20.18.174:8084/fc/"
    SERVICE_MALL_PREFIX = "http://10.20.37.226:8101/"
    HEADERS = {"Content-Type": "application/json", 
                "Referer": "http://10.20.37.227:8088/service-mall/",
                "Accept": "application/json"}

    def __init__(self):
        pass

    def request(self, url, data, is_get_method=True, verify=False, headers={}):
        # print("Will Reuqeuest Interface: {}".format(url))
        # print("Augues as follows: \n{}".format(data))
        if not headers:
            headers = Interfaces.HEADERS
        if is_get_method:
            r = requests.get(url=url, headers=headers, verify=verify, params=data)
        else:
            r = requests.post(url=url, headers=headers, verify=verify, params=data, data=data)
        result = r.json()
        # print("Response: \n{}".format(result))
        return result

    def get_url_prefix(self, url_name="ANALYSIS_FC_PREFIX"):
        return Interfaces.url_name

import time
interfaces = Interfaces()
if __name__ == "__main__":
    url = "http://192.168.200.31:7081/ifs-management/portrait_search"
    data = """{"conditions":null,"keyword":"0","page_no":1,"page_size":20,"access_token":"88720ec3fc0c37d49e77187369010e134e4056d2ee831ea1e0cf93a3275912b0"}"""
    result = interfaces.request(url=url, data=data, is_get_method=False)
    print(result)

