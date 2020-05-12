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
    url = "http://10.20.18.174:9007/chasing/general/get_bond_page_user_daily_data"
    data = '{"fund_account": "88888", "init_date": "20200326"}'
    result = interfaces.request(url=url, data=data, is_get_method=False)
    print(result)

