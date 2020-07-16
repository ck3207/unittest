from interfaces import interfaces
import os

class InsertCouple:
    def __init__(self, url="http://10.26.210.81:8101/market/", name_prefix="couple_", token="", num=1000):
        self.url = url
        self.name_prefix = name_prefix
        self.num = num
        self.token = token
        self.name = self.name_prefix + str(self.num)

    def save_couple(self, url="saveCouponInfo"):
        self.name = self.name_prefix + str(self.num)
        data = {'managerToken': self.token,
                'couponName': self.name, 'startDate': '20200716000000', 'endDate': '20200815235959',
                'receivStartDate': '20200716000000', 'receivEndDate': '20200815235959',
                'couponBase': '[{"deductionAmount":10,"threshold":100,"quantity":10000,"limitNum":1,"accumulation":1}]',
                'synopsis': 'synopsis', 'promotionChannel': 1, 'useScope': 1, 'gainWay': 1,
                'shortMessage': 2, 'memberType': 0}
        res = interfaces.request(url="".join([self.url, url]), data=data, is_get_method=False)
        couple_id = eval(res.get("data").get("couponBase"))[0].get("id")
        self.coupons_on_sale(data, couple_id, self.name)
        self.num += 1

    def coupons_on_sale(self, data, couple_id, name, url="couponsOnSale"):
        data = {"managerToken": data.get("managerToken"), "status": 1, "id": couple_id}
        res = interfaces.request(url="".join([self.url, url]), data=data, is_get_method=False)
        if res.get("errorCode") == "200":
            print("上架优惠券{0}|{1}成功".format(name, couple_id))

if __name__ == "__main__":
    insert_couple = InsertCouple(token="dd18d36c6c81300274ad4da3c3ce1a9e2590f3128fce67d2fa21b3b15e0a1738", num=1040)
    cycle = 10
    while cycle:
        insert_couple.save_couple()
        cycle -= 1
