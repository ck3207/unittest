from interfaces import interfaces

url = "http://10.20.34.83/vipstu/v1/func_mobile_login_register_by_tenant"
data = {
    "tenant_key": "228a888fb5e442fbad79667bec844a82",
    "customer_avatar": "123",
    "password": "e10adc3949ba59abbe56e057f20f883e"
}
# for mobile in range(19990006208, 19999999999):
cnt = 2
while cnt:
    mobile = 19990006208
    data["mobile"] = str(mobile)
    data["customer_name"] = str(mobile)[:3] + "*****" + str(mobile)[-4:]
    # result = interfaces.request(url=url, data=str(data), is_get_method=False, verify=False)
    interfaces.request(url=url, data=str(data), is_get_method=False, verify=False)
    interfaces.request(url=url, data=str(data), is_get_method=False, verify=False)
    # try:
        # if result.get("data")[0].get("error_no") != "0":
            # print("Request Param:\n{0}".format(data))
            # print("Response: \n{0}".format(result))
        # else:
            # print(result.get("data")[0].get("error_no"))
    # except Exception as e:
        # print(str(e))
        # print("Request Param:\n{0}".format(data))
        # print("Response: \n{0}".format(result))
    # time.sleep(10)
    cnt -= 1