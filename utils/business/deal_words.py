words_1 = """managerToken=dd18d36c6c81300274ad4da3c3ce1a9e2590f3128fce67d2fa21b3b15e0a1738&couponName=ck0716-5&startDate=20200716000000&endDate=20200815235959&receivStartDate=20200716000000&receivEndDate=20200815235959&couponBase=%5B%7B%22deductionAmount%22%3A0%2C%22threshold%22%3A0%2C%22quantity%22%3A0%2C%22limitNum%22%3A0%2C%22accumulation%22%3A1%7D%5D"""
words_2 = """busin_account=70201419&company_id=91000&businsys_no=1000&begin_date=20200512&end_date=20200612&password=1&user_id=538476&position_str=0&request_num=1000&combi_code=ZH210039&not_need_import=1&user_token=976e1f13b8194aa59e2f7f78d8caf704&access_token=94F80FE10DDB4755B4BFDF5AA324503820200612105207416D4F7D&sysno=1&reqType=trans"""
words_dict = {}
words_dict.setdefault("words_1", {})
words_dict.setdefault("words_2", {})

for data in words_1.split("&"):
    k, v = data.split("=")
    words_dict.get("words_1").setdefault(k.strip(), v.strip())
print(words_dict.get("words_1"))

# for data in words_2.split("&"):
#     k, v = data.split("=")
#     if k not in words_dict.get("words_1"):
#         print("There is no argue {} in words_1.".format(k))
#     elif v != words_dict.get("words_1").get(k):
#         print("Different argue[{0}]: value of words_1 is {1}, value of words_2 is {2}".\
#               format(k, words_dict.get("words_1").get(k), v))


