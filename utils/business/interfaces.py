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
    url = "http://10.20.37.228/trans/comentrust_detail_order_enter?company_id=91000&businsys_no=1000&busin_account=70201421&op_entrust_way=7&aasexch_type=A01&prod_code=600000&entrust_amount=100&entrust_price=10.45&entrust_bs=1&entrust_prop=U&entrust_type=0&entrust_reason=%E8%B0%83%E4%BB%93%E7%90%86%E7%94%B1-%E6%B5%A6%E5%8F%91%E9%93%B6%E8%A1%8C&user_token=ef3f1d5f5b8d46c18f37787e80088cee&password_type=2&password=UQkmc599Ji7I%2FyXyLu1VG5musuO9VwrkSlUS6Wr4BgVG%2BhVNnP4h%2Bv9VODXMmHQ6mZPoFt9DkyxmrJ%2BS2UaS4wIDMVwVvtms7qccD9bbAgUjH2zal11cuhrl9vlM%2F6IbZXb0AhtXuUCOpCPJOyUK1wAG6ezf1dCnZeo16r07BN0%3D&access_token=EECDFF1F11674E87A2390543A6E3C3E020200617091652416D4F7D&sysno=1&reqType=trans"
    data = {}
    count = 20
    while count:
        result = interfaces.request(url=url, data=data, is_get_method=True)
        count -= 1
        print(result)
        time.sleep(1)

