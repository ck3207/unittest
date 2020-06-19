words_1 = """company_id=91000&businsys_no=1000&busin_account=70201421&op_entrust_way=7&aasexch_type=A02&prod_code=000001&entrust_amount=100&entrust_price=12.91&entrust_bs=1&entrust_prop=Q&entrust_type=0&entrust_reason=&user_token=634fbcd3f60f41058508217bb63ca6b3&password_type=2&password=UQkmc599Ji7I%2FyXyLu1VG5musuO9VwrkSlUS6Wr4BgVG%2BhVNnP4h%2Bv9VODXMmHQ6mZPoFt9DkyxmrJ%2BS2UaS4wIDMVwVvtms7qccD9bbAgUjH2zal11cuhrl9vlM%2F6IbZXb0AhtXuUCOpCPJOyUK1wAG6ezf1dCnZeo16r07BN0%3D&access_token=65F91B8B30634F90AC88137D9330186C20200616092213416D4F7D&sysno=1&reqType=trans"""
words_2 = """busin_account=70201419&company_id=91000&businsys_no=1000&begin_date=20200512&end_date=20200612&password=1&user_id=538476&position_str=0&request_num=1000&combi_code=ZH210039&not_need_import=1&user_token=976e1f13b8194aa59e2f7f78d8caf704&access_token=94F80FE10DDB4755B4BFDF5AA324503820200612105207416D4F7D&sysno=1&reqType=trans"""
words_dict = {}
words_dict.setdefault("words_1", {})
words_dict.setdefault("words_2", {})

for data in words_1.split("&"):
    k, v = data.split("=")
    words_dict.get("words_1").setdefault(k.strip(), v.strip())
print(words_dict.get("words_1"))

for data in words_2.split("&"):
    k, v = data.split("=")
    if k not in words_dict.get("words_1"):
        print("There is no argue {} in words_1.".format(k))
    elif v != words_dict.get("words_1").get(k):
        print("Different argue[{0}]: value of words_1 is {1}, value of words_2 is {2}".\
              format(k, words_dict.get("words_1").get(k), v))


