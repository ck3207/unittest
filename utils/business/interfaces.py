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
    url = "http://10.20.37.227:8088/vipstu/v1/func_mobile_login_register_by_tenant"
    data = {
        "tenant_key": "228a888fb5e442fbad79667bec844a82",
        "customer_avatar": "123",
        "password": "e10adc3949ba59abbe56e057f20f883e"
    }
    for mobile in range(19990006208, 19999999999):
        data["mobile"] = str(mobile)
        data["customer_name"] = str(mobile)[:3]+"*****"+str(mobile)[-4:]
        result = interfaces.request(url=url, data=str(data), is_get_method=False, verify=False)
        try:
            if result.get("data")[0].get("error_no") != "0":
                print("Request Param:\n{0}".format(data))
                print("Response: \n{0}".format(result))
        except Exception as e:
            print(str(e))
            print("Request Param:\n{0}".format(data))
            print("Response: \n{0}".format(result))
        time.sleep(10)